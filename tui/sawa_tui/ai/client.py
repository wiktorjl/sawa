"""Z.AI API client with streaming support."""

import json
import logging
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Any

import httpx

from sawa_tui.config import get_zai_api_key, get_zai_api_url

logger = logging.getLogger(__name__)


@dataclass
class GlossaryEntry:
    """Parsed glossary entry from AI response."""

    term: str
    official_definition: str
    plain_english: str
    examples: list[str]
    related_terms: list[str]
    learn_more: list[str]
    custom_prompt: str | None = None

    @classmethod
    def from_json(
        cls, term: str, data: dict[str, Any], custom_prompt: str | None = None
    ) -> "GlossaryEntry":
        """Create a GlossaryEntry from parsed JSON data."""
        return cls(
            term=term,
            official_definition=data.get("official_definition", ""),
            plain_english=data.get("plain_english", ""),
            examples=data.get("examples", []),
            related_terms=data.get("related_terms", []),
            learn_more=data.get("learn_more", []),
            custom_prompt=custom_prompt,
        )


class ZAIError(Exception):
    """Error from Z.AI API."""

    def __init__(self, message: str, status_code: int | None = None):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ZAIClient:
    """Client for Z.AI API with streaming support."""

    def __init__(self):
        self.api_key = get_zai_api_key()
        self.api_url = get_zai_api_url()
        self.model = "glm-4.7"
        self.timeout = 60.0

    def is_configured(self) -> bool:
        """Check if API key is configured."""
        return bool(self.api_key)

    def generate_sync(self, messages: list[dict[str, str]]) -> str:
        """
        Generate a response synchronously (non-streaming).

        Args:
            messages: List of message dicts with 'role' and 'content'

        Returns:
            The generated text content

        Raises:
            ZAIError: If the API call fails
        """
        if not self.api_key:
            raise ZAIError("ZAI_API_KEY not configured")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept-Language": "en-US,en",
        }

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.7,
            "max_tokens": 2048,
        }

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(self.api_url, headers=headers, json=payload)

                if response.status_code != 200:
                    error_text = response.text
                    try:
                        error_data = response.json()
                        error_text = error_data.get("error", {}).get("message", error_text)
                    except Exception:
                        pass
                    raise ZAIError(f"API error: {error_text}", response.status_code)

                data = response.json()
                return data["choices"][0]["message"]["content"]

        except httpx.TimeoutException:
            raise ZAIError("Request timed out")
        except httpx.RequestError as e:
            raise ZAIError(f"Connection error: {e}")

    def generate_stream(self, messages: list[dict[str, str]]) -> Generator[str, None, None]:
        """
        Generate a response with streaming.

        Args:
            messages: List of message dicts with 'role' and 'content'

        Yields:
            Text chunks as they arrive

        Raises:
            ZAIError: If the API call fails
        """
        if not self.api_key:
            raise ZAIError("ZAI_API_KEY not configured")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept-Language": "en-US,en",
        }

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.7,
            "max_tokens": 2048,
            "stream": True,
        }

        try:
            with httpx.Client(timeout=self.timeout) as client:
                with client.stream("POST", self.api_url, headers=headers, json=payload) as response:
                    if response.status_code != 200:
                        # Read the error body
                        error_text = ""
                        for chunk in response.iter_text():
                            error_text += chunk
                        raise ZAIError(f"API error: {error_text}", response.status_code)

                    for line in response.iter_lines():
                        if not line:
                            continue
                        if line.startswith("data: "):
                            data_str = line[6:]  # Remove "data: " prefix
                            if data_str.strip() == "[DONE]":
                                break
                            try:
                                data = json.loads(data_str)
                                delta = data.get("choices", [{}])[0].get("delta", {})
                                content = delta.get("content", "")
                                if content:
                                    yield content
                            except json.JSONDecodeError:
                                # Skip malformed chunks
                                continue

        except httpx.TimeoutException:
            raise ZAIError("Request timed out")
        except httpx.RequestError as e:
            raise ZAIError(f"Connection error: {e}")

    def generate_glossary_entry(
        self,
        term: str,
        custom_instructions: str = "",
        stream_callback: Callable[[str], None] | None = None,
    ) -> GlossaryEntry:
        """
        Generate a glossary entry for a term.

        Args:
            term: The financial term to define
            custom_instructions: Optional custom instructions
            stream_callback: Optional callback(chunk) for streaming updates

        Returns:
            Parsed GlossaryEntry

        Raises:
            ZAIError: If generation or parsing fails
        """
        from sawa_tui.ai.prompts import build_glossary_prompt

        messages = build_glossary_prompt(term, custom_instructions)

        if stream_callback:
            # Use streaming
            full_response = ""
            for chunk in self.generate_stream(messages):
                full_response += chunk
                stream_callback(chunk)
        else:
            # Non-streaming
            full_response = self.generate_sync(messages)

        # Parse the JSON response
        try:
            # Clean up response - remove any markdown code blocks if present
            content = full_response.strip()
            if content.startswith("```"):
                # Remove markdown code block
                lines = content.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                content = "\n".join(lines)

            data = json.loads(content)
            return GlossaryEntry.from_json(
                term=term,
                data=data,
                custom_prompt=custom_instructions if custom_instructions else None,
            )
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse glossary response: {e}")
            logger.error(f"Response was: {full_response[:500]}")
            raise ZAIError(f"Failed to parse AI response: {e}")
