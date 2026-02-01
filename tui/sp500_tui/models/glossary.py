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
    def get_cached_definition(term: str) -> CachedDefinition | None:
        """
        Get a cached definition for a term.

        Args:
            term: The term to look up

        Returns:
            CachedDefinition if found, None otherwise
        """
        sql = """
            SELECT term, official_definition, plain_english, examples,
                   related_terms, learn_more, custom_prompt, generated_at, model_used
            FROM glossary_terms
            WHERE term = %(term)s
        """
        rows = execute_query(sql, {"term": term})
        return CachedDefinition.from_row(rows[0]) if rows else None

    @staticmethod
    def save_definition(entry: GlossaryEntry, model: str = "glm-4.7") -> bool:
        """
        Save a glossary definition to the cache.

        Args:
            entry: The GlossaryEntry to save
            model: The model used to generate the definition

        Returns:
            True if saved successfully
        """
        sql = """
            INSERT INTO glossary_terms 
                (term, official_definition, plain_english, examples, 
                 related_terms, learn_more, custom_prompt, model_used)
            VALUES 
                (%(term)s, %(official_definition)s, %(plain_english)s, %(examples)s,
                 %(related_terms)s, %(learn_more)s, %(custom_prompt)s, %(model_used)s)
            ON CONFLICT (term) DO UPDATE SET
                official_definition = EXCLUDED.official_definition,
                plain_english = EXCLUDED.plain_english,
                examples = EXCLUDED.examples,
                related_terms = EXCLUDED.related_terms,
                learn_more = EXCLUDED.learn_more,
                custom_prompt = EXCLUDED.custom_prompt,
                model_used = EXCLUDED.model_used,
                generated_at = CURRENT_TIMESTAMP
        """
        try:
            execute_write(
                sql,
                {
                    "term": entry.term,
                    "official_definition": entry.official_definition,
                    "plain_english": entry.plain_english,
                    "examples": json.dumps(entry.examples),
                    "related_terms": json.dumps(entry.related_terms),
                    "learn_more": json.dumps(entry.learn_more),
                    "custom_prompt": entry.custom_prompt,
                    "model_used": model,
                },
            )
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
    def delete_cached_definition(term: str) -> bool:
        """
        Delete a cached definition (force regeneration).

        Args:
            term: The term whose definition to delete

        Returns:
            True if deleted successfully
        """
        sql = "DELETE FROM glossary_terms WHERE term = %(term)s"
        try:
            execute_write(sql, {"term": term})
            return True
        except Exception as e:
            logger.error(f"Failed to delete cached definition: {e}")
            return False

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
