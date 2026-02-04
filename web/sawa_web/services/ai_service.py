"""Z.AI API service for web interface."""

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from sawa_web.database.connection import execute_query, execute_write

logger = logging.getLogger(__name__)


class ZAIError(Exception):
    """Error from Z.AI API."""

    def __init__(self, message: str, status_code: int | None = None):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


@dataclass
class CompanyOverview:
    """AI-generated company overview."""

    ticker: str
    main_product: str
    revenue_model: str
    headwinds: list[str]
    tailwinds: list[str]
    sector_outlook: str
    competitive_position: str
    generated_at: datetime
    model_used: str

    @classmethod
    def from_json(cls, ticker: str, data: dict[str, Any], model: str = "glm-4.7") -> "CompanyOverview":
        """Create from parsed JSON API response."""
        return cls(
            ticker=ticker.upper(),
            main_product=data.get("main_product", ""),
            revenue_model=data.get("revenue_model", ""),
            headwinds=data.get("headwinds", []),
            tailwinds=data.get("tailwinds", []),
            sector_outlook=data.get("sector_outlook", ""),
            competitive_position=data.get("competitive_position", ""),
            generated_at=datetime.now(),
            model_used=model,
        )


COMPANY_OVERVIEW_SYSTEM_PROMPT = (
    "You are a senior equity research analyst who provides clear, actionable company "
    "analysis for investors. Your analysis should be factual, balanced, and based on "
    "publicly available information. Always return valid JSON without any markdown "
    "formatting or code blocks."
)

COMPANY_OVERVIEW_USER_PROMPT = """Analyze the company: {ticker} ({company_name})
Sector: {sector}

Return a JSON object with this exact structure:
{{
  "main_product": "A 2-3 sentence description of the company's most important product or service. What generates the majority of their revenue or defines their market position?",

  "revenue_model": "A 2-3 sentence explanation of how the company makes money. Include key revenue streams, business model type (subscription, transaction, advertising, etc.), and any notable pricing dynamics.",

  "headwinds": [
    "Major challenge or risk #1 facing the company (1-2 sentences)",
    "Major challenge or risk #2 (1-2 sentences)",
    "Major challenge or risk #3 (1-2 sentences)"
  ],

  "tailwinds": [
    "Growth driver or opportunity #1 (1-2 sentences)",
    "Growth driver or opportunity #2 (1-2 sentences)",
    "Growth driver or opportunity #3 (1-2 sentences)"
  ],

  "sector_outlook": "2-3 sentences on how the overall sector/industry is performing. Include recent trends, growth prospects, and any macro factors affecting the sector.",

  "competitive_position": "2-3 sentences on where this company stands relative to competitors. Include market share context, competitive advantages (moats), and key differentiators."
}}

Guidelines:
- Be specific with examples where possible (e.g., "iPhone generates ~50% of revenue")
- For headwinds/tailwinds, prioritize the most impactful factors
- Mention specific competitors by name in competitive_position
- Keep each section focused and actionable for investors
- If information is uncertain, indicate with "reportedly" or "estimated"

{custom_instructions}

Return ONLY the JSON object, no additional text or formatting."""


async def get_zai_api_key(user_id: int | None = None) -> str | None:
    """
    Get the Z.AI API key.

    Checks in order:
    1. Environment variable ZAI_API_KEY
    2. Database user_settings (if user_id provided)
    3. Database default_settings

    Returns:
        API key string or None if not configured
    """
    # Check environment first
    env_key = os.environ.get("ZAI_API_KEY")
    if env_key:
        return env_key

    # Try to get from database settings
    try:
        if user_id is not None:
            result = await execute_query(
                "SELECT value FROM user_settings WHERE user_id = $1 AND key = 'zai_api_key'",
                user_id,
                fetch_one=True,
            )
            if result and result.get("value"):
                return result["value"]

        # Try default settings
        result = await execute_query(
            "SELECT value FROM default_settings WHERE key = 'zai_api_key'",
            fetch_one=True,
        )
        if result and result.get("value"):
            return result["value"]
    except Exception as e:
        logger.warning(f"Failed to get API key from database: {e}")

    return None


def get_zai_api_url() -> str:
    """Get the Z.AI API endpoint URL."""
    return os.environ.get("ZAI_API_URL", "https://api.z.ai/api/coding/paas/v4/chat/completions")


async def generate_company_overview(
    ticker: str,
    company_name: str,
    sector: str | None = None,
    user_id: int | None = None,
    custom_instructions: str = "",
) -> CompanyOverview:
    """
    Generate a company overview using Z.AI.

    Args:
        ticker: Stock ticker symbol
        company_name: Full company name
        sector: Industry/sector description
        user_id: User ID for API key lookup
        custom_instructions: Optional custom instructions

    Returns:
        Generated CompanyOverview

    Raises:
        ZAIError: If generation fails
    """
    api_key = await get_zai_api_key(user_id)
    if not api_key:
        raise ZAIError("ZAI_API_KEY not configured. Please set it in Settings.")

    api_url = get_zai_api_url()
    model = "glm-4.7"

    custom_text = ""
    if custom_instructions:
        custom_text = f"\nAdditional instructions: {custom_instructions}"

    messages = [
        {"role": "system", "content": COMPANY_OVERVIEW_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": COMPANY_OVERVIEW_USER_PROMPT.format(
                ticker=ticker,
                company_name=company_name,
                sector=sector or "Unknown",
                custom_instructions=custom_text,
            ),
        },
    ]

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept-Language": "en-US,en",
    }

    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.7,
        "max_tokens": 2048,
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(api_url, headers=headers, json=payload)

            if response.status_code != 200:
                error_text = response.text
                try:
                    error_data = response.json()
                    error_text = error_data.get("error", {}).get("message", error_text)
                except Exception:
                    pass
                raise ZAIError(f"API error: {error_text}", response.status_code)

            data = response.json()
            content = data["choices"][0]["message"]["content"]

            # Clean up response - remove any markdown code blocks if present
            content = content.strip()
            if content.startswith("```"):
                lines = content.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                content = "\n".join(lines)

            parsed = json.loads(content)
            return CompanyOverview.from_json(ticker, parsed, model)

    except httpx.TimeoutException:
        raise ZAIError("Request timed out")
    except httpx.RequestError as e:
        raise ZAIError(f"Connection error: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse overview response: {e}")
        raise ZAIError(f"Failed to parse AI response: {e}")


@dataclass
class GlossaryDefinition:
    """AI-generated glossary definition."""

    term: str
    official_definition: str
    plain_english: str
    examples: list[str]
    related_terms: list[str]
    learn_more: list[str]
    generated_at: datetime
    model_used: str
    custom_prompt: str | None = None

    @classmethod
    def from_json(cls, term: str, data: dict[str, Any], model: str = "glm-4.7", custom_prompt: str | None = None) -> "GlossaryDefinition":
        """Create from parsed JSON API response."""
        return cls(
            term=term,
            official_definition=data.get("official_definition", ""),
            plain_english=data.get("plain_english", ""),
            examples=data.get("examples", []),
            related_terms=data.get("related_terms", []),
            learn_more=data.get("learn_more", []),
            generated_at=datetime.now(),
            model_used=model,
            custom_prompt=custom_prompt,
        )


GLOSSARY_SYSTEM_PROMPT = (
    "You are a financial education expert who explains complex financial concepts "
    "in clear, accessible language. Your explanations should be accurate, practical, "
    "and help investors understand how to use these concepts in their analysis. "
    "Always return valid JSON without any markdown formatting or code blocks."
)

GLOSSARY_USER_PROMPT = """Define the financial term: "{term}"

Return a JSON object with this exact structure:
{{
  "official_definition": "A precise, technical definition of the term as it would appear in a finance textbook or CFA curriculum (2-3 sentences).",

  "plain_english": "An explanation in simple, everyday language that someone without a finance background could understand. Use analogies where helpful (2-3 sentences).",

  "examples": [
    "A concrete example showing how this concept works with real numbers or a realistic scenario",
    "Another practical example, ideally from a different context or industry"
  ],

  "related_terms": [
    "Related Term 1",
    "Related Term 2",
    "Related Term 3"
  ],

  "learn_more": [
    "Brief suggestion for where to learn more (e.g., 'Investopedia', 'Company annual reports', 'SEC filings')"
  ]
}}

{custom_instructions}

Return ONLY the JSON object, no additional text or formatting."""

GLOSSARY_REGEN_PROMPTS = {
    "technical": "Make the explanation more technical and detailed, suitable for finance professionals.",
    "simple": "Make the explanation simpler and more accessible, suitable for complete beginners.",
    "examples": "Provide more detailed, real-world examples with specific numbers and company names.",
}


async def generate_glossary_definition(
    term: str,
    user_id: int | None = None,
    custom_prompt: str | None = None,
    regen_type: str | None = None,
) -> GlossaryDefinition:
    """
    Generate a glossary definition using Z.AI.

    Args:
        term: The financial term to define
        user_id: User ID for API key lookup
        custom_prompt: Optional custom instructions
        regen_type: Type of regeneration (technical, simple, examples)

    Returns:
        Generated GlossaryDefinition

    Raises:
        ZAIError: If generation fails
    """
    api_key = await get_zai_api_key(user_id)
    if not api_key:
        raise ZAIError("ZAI_API_KEY not configured. Please set it in Settings.")

    api_url = get_zai_api_url()
    model = "glm-4.7"

    # Build custom instructions
    custom_text = ""
    if regen_type and regen_type in GLOSSARY_REGEN_PROMPTS:
        custom_text = f"\nAdditional instructions: {GLOSSARY_REGEN_PROMPTS[regen_type]}"
    elif custom_prompt:
        custom_text = f"\nAdditional instructions: {custom_prompt}"

    messages = [
        {"role": "system", "content": GLOSSARY_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": GLOSSARY_USER_PROMPT.format(
                term=term,
                custom_instructions=custom_text,
            ),
        },
    ]

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept-Language": "en-US,en",
    }

    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.7,
        "max_tokens": 1500,
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(api_url, headers=headers, json=payload)

            if response.status_code != 200:
                error_text = response.text
                try:
                    error_data = response.json()
                    error_text = error_data.get("error", {}).get("message", error_text)
                except Exception:
                    pass
                raise ZAIError(f"API error: {error_text}", response.status_code)

            data = response.json()
            content = data["choices"][0]["message"]["content"]

            # Clean up response - remove any markdown code blocks if present
            content = content.strip()
            if content.startswith("```"):
                lines = content.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                content = "\n".join(lines)

            parsed = json.loads(content)
            return GlossaryDefinition.from_json(term, parsed, model, custom_prompt or regen_type)

    except httpx.TimeoutException:
        raise ZAIError("Request timed out")
    except httpx.RequestError as e:
        raise ZAIError(f"Connection error: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse glossary response: {e}")
        raise ZAIError(f"Failed to parse AI response: {e}")


async def save_glossary_definition(definition: GlossaryDefinition, user_id: int | None = None) -> bool:
    """
    Save a glossary definition to the database.

    Args:
        definition: The GlossaryDefinition to save
        user_id: Optional user ID for user-specific override

    Returns:
        True if saved successfully
    """
    try:
        if user_id is None:
            # Shared definition
            existing = await execute_query(
                "SELECT id FROM glossary_terms WHERE term = $1 AND user_id IS NULL",
                definition.term,
                fetch_one=True,
            )

            if existing:
                await execute_write(
                    """
                    UPDATE glossary_terms SET
                        official_definition = $2,
                        plain_english = $3,
                        examples = $4,
                        related_terms = $5,
                        learn_more = $6,
                        custom_prompt = $7,
                        model_used = $8,
                        generated_at = CURRENT_TIMESTAMP
                    WHERE term = $1 AND user_id IS NULL
                    """,
                    definition.term,
                    definition.official_definition,
                    definition.plain_english,
                    json.dumps(definition.examples),
                    json.dumps(definition.related_terms),
                    json.dumps(definition.learn_more),
                    definition.custom_prompt,
                    definition.model_used,
                )
            else:
                await execute_write(
                    """
                    INSERT INTO glossary_terms
                        (term, official_definition, plain_english, examples, related_terms,
                         learn_more, custom_prompt, model_used, user_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL)
                    """,
                    definition.term,
                    definition.official_definition,
                    definition.plain_english,
                    json.dumps(definition.examples),
                    json.dumps(definition.related_terms),
                    json.dumps(definition.learn_more),
                    definition.custom_prompt,
                    definition.model_used,
                )
        else:
            # User-specific definition
            existing = await execute_query(
                "SELECT id FROM glossary_terms WHERE term = $1 AND user_id = $2",
                definition.term,
                user_id,
                fetch_one=True,
            )

            if existing:
                await execute_write(
                    """
                    UPDATE glossary_terms SET
                        official_definition = $3,
                        plain_english = $4,
                        examples = $5,
                        related_terms = $6,
                        learn_more = $7,
                        custom_prompt = $8,
                        model_used = $9,
                        generated_at = CURRENT_TIMESTAMP
                    WHERE term = $1 AND user_id = $2
                    """,
                    definition.term,
                    user_id,
                    definition.official_definition,
                    definition.plain_english,
                    json.dumps(definition.examples),
                    json.dumps(definition.related_terms),
                    json.dumps(definition.learn_more),
                    definition.custom_prompt,
                    definition.model_used,
                )
            else:
                await execute_write(
                    """
                    INSERT INTO glossary_terms
                        (term, official_definition, plain_english, examples, related_terms,
                         learn_more, custom_prompt, model_used, user_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    definition.term,
                    definition.official_definition,
                    definition.plain_english,
                    json.dumps(definition.examples),
                    json.dumps(definition.related_terms),
                    json.dumps(definition.learn_more),
                    definition.custom_prompt,
                    definition.model_used,
                    user_id,
                )

        return True
    except Exception as e:
        logger.error(f"Failed to save glossary definition: {e}")
        return False


async def save_company_overview(overview: CompanyOverview, user_id: int | None = None) -> bool:
    """
    Save a company overview to the database.

    Args:
        overview: The CompanyOverview to save
        user_id: Optional user ID for user-specific override

    Returns:
        True if saved successfully
    """
    ticker = overview.ticker.upper()

    try:
        if user_id is None:
            # Check if shared overview exists
            existing = await execute_query(
                "SELECT id FROM company_overviews WHERE ticker = $1 AND user_id IS NULL",
                ticker,
                fetch_one=True,
            )

            if existing:
                await execute_write(
                    """
                    UPDATE company_overviews SET
                        main_product = $2,
                        revenue_model = $3,
                        headwinds = $4,
                        tailwinds = $5,
                        sector_outlook = $6,
                        competitive_position = $7,
                        model_used = $8,
                        generated_at = CURRENT_TIMESTAMP
                    WHERE ticker = $1 AND user_id IS NULL
                    """,
                    ticker,
                    overview.main_product,
                    overview.revenue_model,
                    json.dumps(overview.headwinds),
                    json.dumps(overview.tailwinds),
                    overview.sector_outlook,
                    overview.competitive_position,
                    overview.model_used,
                )
            else:
                await execute_write(
                    """
                    INSERT INTO company_overviews
                        (ticker, main_product, revenue_model, headwinds, tailwinds,
                         sector_outlook, competitive_position, model_used, user_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL)
                    """,
                    ticker,
                    overview.main_product,
                    overview.revenue_model,
                    json.dumps(overview.headwinds),
                    json.dumps(overview.tailwinds),
                    overview.sector_outlook,
                    overview.competitive_position,
                    overview.model_used,
                )
        else:
            # User-specific override
            existing = await execute_query(
                "SELECT id FROM company_overviews WHERE ticker = $1 AND user_id = $2",
                ticker,
                user_id,
                fetch_one=True,
            )

            if existing:
                await execute_write(
                    """
                    UPDATE company_overviews SET
                        main_product = $3,
                        revenue_model = $4,
                        headwinds = $5,
                        tailwinds = $6,
                        sector_outlook = $7,
                        competitive_position = $8,
                        model_used = $9,
                        generated_at = CURRENT_TIMESTAMP
                    WHERE ticker = $1 AND user_id = $2
                    """,
                    ticker,
                    user_id,
                    overview.main_product,
                    overview.revenue_model,
                    json.dumps(overview.headwinds),
                    json.dumps(overview.tailwinds),
                    overview.sector_outlook,
                    overview.competitive_position,
                    overview.model_used,
                )
            else:
                await execute_write(
                    """
                    INSERT INTO company_overviews
                        (ticker, main_product, revenue_model, headwinds, tailwinds,
                         sector_outlook, competitive_position, model_used, user_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    ticker,
                    overview.main_product,
                    overview.revenue_model,
                    json.dumps(overview.headwinds),
                    json.dumps(overview.tailwinds),
                    overview.sector_outlook,
                    overview.competitive_position,
                    overview.model_used,
                    user_id,
                )

        return True
    except Exception as e:
        logger.error(f"Failed to save company overview: {e}")
        return False
