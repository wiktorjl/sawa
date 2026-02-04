"""Configuration management for ORION web dashboard."""

import os
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    # Database
    database_url: str = os.environ.get(
        "DATABASE_URL",
        "postgresql://localhost:5432/sawa"
    )

    # Session
    secret_key: str = os.environ.get(
        "SECRET_KEY",
        "change-me-in-production-use-a-real-secret-key"
    )
    session_cookie_name: str = "orion_session"
    session_max_age: int = 60 * 60 * 24 * 7  # 7 days

    # Server
    host: str = os.environ.get("HOST", "0.0.0.0")
    port: int = int(os.environ.get("PORT", "8000"))
    debug: bool = os.environ.get("DEBUG", "false").lower() == "true"

    # AI API (Z.AI)
    zai_api_key: str | None = os.environ.get("ZAI_API_KEY")
    zai_api_url: str = os.environ.get(
        "ZAI_API_URL",
        "https://api.z.ai/api/coding/paas/v4/chat/completions"
    )

    @property
    def asyncpg_url(self) -> str:
        """Convert DATABASE_URL to asyncpg format if needed."""
        url = self.database_url
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql://", 1)
        if url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql://", 1)
        return url


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
