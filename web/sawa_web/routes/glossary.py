"""Glossary routes for financial term definitions."""

import json
import logging
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sawa_web.database.connection import execute_query, execute_write
from sawa_web.dependencies import CurrentUser
from sawa_web.services.ai_service import (
    ZAIError,
    generate_glossary_definition,
    save_glossary_definition,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/glossary", tags=["glossary"])

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Category short codes for badges
CATEGORY_CODES = {
    "Valuation": "VAL",
    "Profitability": "PRF",
    "Liquidity": "LIQ",
    "Leverage": "LEV",
    "Cash Flow": "CF",
    "Dividends": "DIV",
    "Growth": "GRW",
    "Trading": "TRD",
}


@router.get("", response_class=HTMLResponse)
async def glossary_page(request: Request, user: CurrentUser):
    """Render the main glossary page."""
    # Get all terms grouped by category
    terms = await execute_query(
        """
        SELECT term, category, source
        FROM glossary_term_list
        ORDER BY category, term
        """,
    )

    # Group by category
    categories = {}
    for term in (terms or []):
        cat = term["category"] or "Other"
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(term)

    return templates.TemplateResponse(
        "glossary/index.html",
        {
            "request": request,
            "user": user,
            "active_page": "glossary",
            "page_title": "Glossary",
            "categories": categories,
            "category_codes": CATEGORY_CODES,
        },
    )


@router.get("/terms", response_class=HTMLResponse)
async def term_list(request: Request, user: CurrentUser, q: str = "", category: str = ""):
    """Get filtered term list partial."""
    query = """
        SELECT term, category, source
        FROM glossary_term_list
        WHERE 1=1
    """
    params = []
    param_idx = 1

    if q:
        query += f" AND term ILIKE ${param_idx}"
        params.append(f"%{q}%")
        param_idx += 1

    if category:
        query += f" AND category = ${param_idx}"
        params.append(category)
        param_idx += 1

    query += " ORDER BY category, term"

    terms = await execute_query(query, *params)

    # Group by category
    categories = {}
    for term in (terms or []):
        cat = term["category"] or "Other"
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(term)

    return templates.TemplateResponse(
        "glossary/partials/term_list.html",
        {
            "request": request,
            "categories": categories,
            "category_codes": CATEGORY_CODES,
            "search_query": q,
        },
    )


@router.get("/term/{term:path}", response_class=HTMLResponse)
async def get_definition(request: Request, user: CurrentUser, term: str):
    """Get definition for a term (cached or empty state)."""
    # Check for cached definition (user-specific first, then shared)
    definition = await execute_query(
        """
        SELECT official_definition, plain_english, examples, related_terms,
               learn_more, custom_prompt, generated_at, model_used
        FROM glossary_terms
        WHERE term = $1 AND (user_id = $2 OR user_id IS NULL)
        ORDER BY CASE WHEN user_id IS NOT NULL THEN 0 ELSE 1 END
        LIMIT 1
        """,
        term,
        user["id"],
        fetch_one=True,
    )

    # Parse JSONB fields if they're strings
    if definition:
        definition = dict(definition)
        for field in ["examples", "related_terms", "learn_more"]:
            if definition.get(field) and isinstance(definition[field], str):
                try:
                    definition[field] = json.loads(definition[field])
                except json.JSONDecodeError:
                    definition[field] = []

    return templates.TemplateResponse(
        "glossary/partials/definition.html",
        {
            "request": request,
            "term": term,
            "definition": definition,
        },
    )


@router.post("/term/{term:path}/generate", response_class=HTMLResponse)
async def generate_definition(request: Request, user: CurrentUser, term: str):
    """Generate AI definition for a term."""
    try:
        definition = await generate_glossary_definition(
            term=term,
            user_id=user["id"],
        )

        # Save to database
        await save_glossary_definition(definition)

        # Return the definition partial
        return templates.TemplateResponse(
            "glossary/partials/definition.html",
            {
                "request": request,
                "term": term,
                "definition": {
                    "official_definition": definition.official_definition,
                    "plain_english": definition.plain_english,
                    "examples": definition.examples,
                    "related_terms": definition.related_terms,
                    "learn_more": definition.learn_more,
                    "generated_at": definition.generated_at,
                    "model_used": definition.model_used,
                },
                "just_generated": True,
            },
        )

    except ZAIError as e:
        logger.error(f"Failed to generate definition for {term}: {e}")
        return HTMLResponse(
            f'<div class="alert alert-danger">Failed to generate definition: {e.message}</div>',
            status_code=500,
        )
    except Exception as e:
        logger.error(f"Unexpected error generating definition for {term}: {e}")
        return HTMLResponse(
            '<div class="alert alert-danger">An unexpected error occurred. Please try again.</div>',
            status_code=500,
        )


@router.post("/term/{term:path}/regenerate", response_class=HTMLResponse)
async def regenerate_definition(
    request: Request,
    user: CurrentUser,
    term: str,
    regen_type: str = Form(""),
    custom_prompt: str = Form(""),
):
    """Regenerate definition with options."""
    try:
        definition = await generate_glossary_definition(
            term=term,
            user_id=user["id"],
            regen_type=regen_type if regen_type else None,
            custom_prompt=custom_prompt if custom_prompt else None,
        )

        # Save to database
        await save_glossary_definition(definition)

        return templates.TemplateResponse(
            "glossary/partials/definition.html",
            {
                "request": request,
                "term": term,
                "definition": {
                    "official_definition": definition.official_definition,
                    "plain_english": definition.plain_english,
                    "examples": definition.examples,
                    "related_terms": definition.related_terms,
                    "learn_more": definition.learn_more,
                    "generated_at": definition.generated_at,
                    "model_used": definition.model_used,
                    "custom_prompt": definition.custom_prompt,
                },
                "just_generated": True,
            },
        )

    except ZAIError as e:
        logger.error(f"Failed to regenerate definition for {term}: {e}")
        return HTMLResponse(
            f'<div class="alert alert-danger">Failed to regenerate definition: {e.message}</div>',
            status_code=500,
        )
    except Exception as e:
        logger.error(f"Unexpected error regenerating definition for {term}: {e}")
        return HTMLResponse(
            '<div class="alert alert-danger">An unexpected error occurred. Please try again.</div>',
            status_code=500,
        )


@router.post("/terms/add", response_class=HTMLResponse)
async def add_term(
    request: Request,
    user: CurrentUser,
    term: str = Form(...),
    category: str = Form("Other"),
):
    """Add a user-defined term."""
    term = term.strip()
    if not term:
        return HTMLResponse(
            '<div class="alert alert-danger">Term cannot be empty</div>',
            status_code=400,
        )

    # Check if term already exists
    existing = await execute_query(
        "SELECT 1 FROM glossary_term_list WHERE term = $1",
        term,
        fetch_one=True,
    )

    if existing:
        return HTMLResponse(
            '<div class="alert alert-warning">Term already exists</div>',
            status_code=400,
        )

    # Add the term
    await execute_write(
        """
        INSERT INTO glossary_term_list (term, category, source)
        VALUES ($1, $2, 'user')
        """,
        term,
        category,
    )

    # Return updated term list
    return await term_list(request, user)


@router.delete("/terms/{term:path}", response_class=HTMLResponse)
async def delete_term(request: Request, user: CurrentUser, term: str):
    """Delete a user-added term."""
    # Only delete user-added terms
    result = await execute_write(
        """
        DELETE FROM glossary_term_list
        WHERE term = $1 AND source = 'user'
        """,
        term,
    )

    # Also delete any cached definitions
    await execute_write(
        "DELETE FROM glossary_terms WHERE term = $1",
        term,
    )

    return HTMLResponse('<div class="alert alert-success">Term deleted</div>')
