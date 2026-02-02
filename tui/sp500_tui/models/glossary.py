"""Glossary data model and CRUD operations."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sp500_tui.ai.client import GlossaryEntry
from sp500_tui.database import execute_query, execute_write

logger = logging.getLogger(__name__)


@dataclass
class GlossaryTerm:
    """Represents a term in the glossary list."""

    term: str
    category: str | None = None
    source: str = "curated"  # 'curated', 'user', 'extracted'
    has_definition: bool = False

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "GlossaryTerm":
        """Create a GlossaryTerm from a database row."""
        return cls(
            term=row["term"],
            category=row.get("category"),
            source=row.get("source", "curated"),
            has_definition=row.get("has_definition", False),
        )


@dataclass
class CachedDefinition:
    """Cached glossary definition from database."""

    term: str
    official_definition: str
    plain_english: str
    examples: list[str]
    related_terms: list[str]
    learn_more: list[str]
    custom_prompt: str | None
    generated_at: datetime
    model_used: str
    is_user_override: bool = False

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "CachedDefinition":
        """Create a CachedDefinition from a database row."""
        return cls(
            term=row["term"],
            official_definition=row.get("official_definition", ""),
            plain_english=row.get("plain_english", ""),
            examples=row.get("examples", []) or [],
            related_terms=row.get("related_terms", []) or [],
            learn_more=row.get("learn_more", []) or [],
            custom_prompt=row.get("custom_prompt"),
            generated_at=row.get("generated_at", datetime.now()),
            model_used=row.get("model_used", "unknown"),
            is_user_override=row.get("user_id") is not None,
        )

    def to_glossary_entry(self) -> GlossaryEntry:
        """Convert to GlossaryEntry for display."""
        return GlossaryEntry(
            term=self.term,
            official_definition=self.official_definition,
            plain_english=self.plain_english,
            examples=self.examples,
            related_terms=self.related_terms,
            learn_more=self.learn_more,
            custom_prompt=self.custom_prompt,
        )


class GlossaryManager:
    """Manager for glossary CRUD operations."""

    @staticmethod
    def get_all_terms(search: str = "") -> list[GlossaryTerm]:
        """
        Get all terms from the glossary list, optionally filtered by search.

        Args:
            search: Optional search string to filter terms

        Returns:
            List of GlossaryTerm objects
        """
        if search:
            sql = """
                SELECT tl.term, tl.category, tl.source,
                       EXISTS(SELECT 1 FROM glossary_terms gt WHERE gt.term = tl.term) as has_definition
                FROM glossary_term_list tl
                WHERE tl.term ILIKE %(search)s
                ORDER BY tl.category, tl.term
            """
            rows = execute_query(sql, {"search": f"%{search}%"})
        else:
            sql = """
                SELECT tl.term, tl.category, tl.source,
                       EXISTS(SELECT 1 FROM glossary_terms gt WHERE gt.term = tl.term) as has_definition
                FROM glossary_term_list tl
                ORDER BY tl.category, tl.term
            """
            rows = execute_query(sql)

        return [GlossaryTerm.from_row(row) for row in rows]

    @staticmethod
    def get_cached_definition(term: str, user_id: int | None = None) -> CachedDefinition | None:
        """
        Get a cached definition for a term.

        Two-tier lookup:
        1. If user_id provided, check for user override first
        2. Fall back to shared definition (user_id IS NULL)

        Args:
            term: The term to look up
            user_id: Optional user ID for user-specific overrides

        Returns:
            CachedDefinition if found, None otherwise
        """
        if user_id is not None:
            # Try user override first
            sql = """
                SELECT term, official_definition, plain_english, examples,
                       related_terms, learn_more, custom_prompt, generated_at, model_used, user_id
                FROM glossary_terms
                WHERE term = %(term)s AND user_id = %(user_id)s
            """
            rows = execute_query(sql, {"term": term, "user_id": user_id})
            if rows:
                return CachedDefinition.from_row(rows[0])

        # Fall back to shared definition
        sql = """
            SELECT term, official_definition, plain_english, examples,
                   related_terms, learn_more, custom_prompt, generated_at, model_used, user_id
            FROM glossary_terms
            WHERE term = %(term)s AND user_id IS NULL
        """
        rows = execute_query(sql, {"term": term})
        return CachedDefinition.from_row(rows[0]) if rows else None

    @staticmethod
    def save_definition(
        entry: GlossaryEntry, model: str = "glm-4.7", user_id: int | None = None
    ) -> bool:
        """
        Save a glossary definition to the cache.

        If user_id is None, saves as shared definition.
        If user_id is provided, saves as user-specific override.

        Args:
            entry: The GlossaryEntry to save
            model: The model used to generate the definition
            user_id: Optional user ID for user-specific override

        Returns:
            True if saved successfully
        """
        if user_id is None:
            # Shared definition - use partial unique index
            # First, check if it exists
            check_sql = "SELECT id FROM glossary_terms WHERE term = %(term)s AND user_id IS NULL"
            existing = execute_query(check_sql, {"term": entry.term})

            if existing:
                # Update existing
                sql = """
                    UPDATE glossary_terms SET
                        official_definition = %(official_definition)s,
                        plain_english = %(plain_english)s,
                        examples = %(examples)s,
                        related_terms = %(related_terms)s,
                        learn_more = %(learn_more)s,
                        custom_prompt = %(custom_prompt)s,
                        model_used = %(model_used)s,
                        generated_at = CURRENT_TIMESTAMP
                    WHERE term = %(term)s AND user_id IS NULL
                """
            else:
                # Insert new
                sql = """
                    INSERT INTO glossary_terms 
                        (term, official_definition, plain_english, examples, 
                         related_terms, learn_more, custom_prompt, model_used, user_id)
                    VALUES 
                        (%(term)s, %(official_definition)s, %(plain_english)s, %(examples)s,
                         %(related_terms)s, %(learn_more)s, %(custom_prompt)s, %(model_used)s, NULL)
                """
        else:
            # User override - use composite unique index
            check_sql = """
                SELECT id FROM glossary_terms 
                WHERE term = %(term)s AND user_id = %(user_id)s
            """
            existing = execute_query(check_sql, {"term": entry.term, "user_id": user_id})

            if existing:
                # Update existing
                sql = """
                    UPDATE glossary_terms SET
                        official_definition = %(official_definition)s,
                        plain_english = %(plain_english)s,
                        examples = %(examples)s,
                        related_terms = %(related_terms)s,
                        learn_more = %(learn_more)s,
                        custom_prompt = %(custom_prompt)s,
                        model_used = %(model_used)s,
                        generated_at = CURRENT_TIMESTAMP
                    WHERE term = %(term)s AND user_id = %(user_id)s
                """
            else:
                # Insert new
                sql = """
                    INSERT INTO glossary_terms 
                        (term, official_definition, plain_english, examples, 
                         related_terms, learn_more, custom_prompt, model_used, user_id)
                    VALUES 
                        (%(term)s, %(official_definition)s, %(plain_english)s, %(examples)s,
                         %(related_terms)s, %(learn_more)s, %(custom_prompt)s, %(model_used)s, %(user_id)s)
                """

        try:
            params = {
                "term": entry.term,
                "official_definition": entry.official_definition,
                "plain_english": entry.plain_english,
                "examples": json.dumps(entry.examples),
                "related_terms": json.dumps(entry.related_terms),
                "learn_more": json.dumps(entry.learn_more),
                "custom_prompt": entry.custom_prompt,
                "model_used": model,
            }
            if user_id is not None:
                params["user_id"] = user_id

            execute_write(sql, params)
            return True
        except Exception as e:
            logger.error(f"Failed to save glossary definition: {e}")
            return False

    @staticmethod
    def add_term(term: str, category: str = "User Added") -> bool:
        """
        Add a new term to the glossary list.

        Args:
            term: The term to add
            category: The category for the term

        Returns:
            True if added successfully
        """
        sql = """
            INSERT INTO glossary_term_list (term, category, source)
            VALUES (%(term)s, %(category)s, 'user')
            ON CONFLICT (term) DO NOTHING
        """
        try:
            result = execute_write(sql, {"term": term, "category": category})
            return result > 0
        except Exception as e:
            logger.error(f"Failed to add term: {e}")
            return False

    @staticmethod
    def delete_term(term: str) -> bool:
        """
        Delete a user-added term from the glossary list.

        Only deletes terms with source='user'.

        Args:
            term: The term to delete

        Returns:
            True if deleted successfully
        """
        # Delete from term list (only user-added)
        sql1 = """
            DELETE FROM glossary_term_list
            WHERE term = %(term)s AND source = 'user'
        """
        # Also delete cached definition
        sql2 = """
            DELETE FROM glossary_terms
            WHERE term = %(term)s
        """
        try:
            result = execute_write(sql1, {"term": term})
            if result > 0:
                execute_write(sql2, {"term": term})
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to delete term: {e}")
            return False

    @staticmethod
    def delete_cached_definition(term: str, user_id: int | None = None) -> bool:
        """
        Delete a cached definition (force regeneration).

        If user_id is None, deletes shared definition.
        If user_id is provided, deletes only user override.

        Args:
            term: The term whose definition to delete
            user_id: Optional user ID for user-specific override

        Returns:
            True if deleted successfully
        """
        if user_id is None:
            sql = "DELETE FROM glossary_terms WHERE term = %(term)s AND user_id IS NULL"
            params = {"term": term}
        else:
            sql = "DELETE FROM glossary_terms WHERE term = %(term)s AND user_id = %(user_id)s"
            params = {"term": term, "user_id": user_id}

        try:
            execute_write(sql, params)
            return True
        except Exception as e:
            logger.error(f"Failed to delete cached definition: {e}")
            return False

    @staticmethod
    def has_user_override(term: str, user_id: int) -> bool:
        """
        Check if a user has a custom override for a term.

        Args:
            term: The term to check
            user_id: User ID

        Returns:
            True if user has a custom override
        """
        sql = "SELECT 1 FROM glossary_terms WHERE term = %(term)s AND user_id = %(user_id)s"
        rows = execute_query(sql, {"term": term, "user_id": user_id})
        return len(rows) > 0

    @staticmethod
    def delete_user_override(term: str, user_id: int) -> bool:
        """
        Delete a user's custom override, reverting to shared definition.

        Args:
            term: The term to revert
            user_id: User ID

        Returns:
            True if deleted successfully
        """
        return GlossaryManager.delete_cached_definition(term, user_id=user_id)

    @staticmethod
    def term_exists(term: str) -> bool:
        """Check if a term exists in the glossary list."""
        sql = "SELECT 1 FROM glossary_term_list WHERE term = %(term)s"
        rows = execute_query(sql, {"term": term})
        return len(rows) > 0

    @staticmethod
    def ensure_term_in_list(term: str) -> None:
        """
        Ensure a term exists in the term list (for related terms navigation).

        If the term doesn't exist, add it as a user term.
        """
        if not GlossaryManager.term_exists(term):
            GlossaryManager.add_term(term, category="Related")
