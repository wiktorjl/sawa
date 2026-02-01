"""Prompt templates for AI-generated content."""

GLOSSARY_SYSTEM_PROMPT = """You are a financial education expert who explains complex financial concepts clearly.
Your goal is to help investors understand financial terminology in a practical, accessible way.
Always return valid JSON without any markdown formatting or code blocks."""

GLOSSARY_USER_PROMPT = """Generate a comprehensive glossary entry for the financial term: "{term}"

Return a JSON object with this exact structure:
{{
  "official_definition": "A formal, textbook-style definition in 1-2 sentences.",
  "plain_english": "What this actually means in simple, conversational terms. Explain like you're talking to a smart friend who isn't a finance expert. Make it relatable and easy to understand.",
  "examples": [
    "A practical example with real or realistic numbers that illustrates the concept",
    "Another example showing a different scenario or use case"
  ],
  "related_terms": ["Related Term 1", "Related Term 2", "Related Term 3", "Related Term 4", "Related Term 5"],
  "learn_more": [
    "https://www.investopedia.com/terms/relevant-page",
    "https://www.investopedia.com/another-relevant-page"
  ]
}}

Guidelines:
- The plain_english section should be engaging and memorable, not dry
- Examples should use realistic numbers (e.g., "Apple at $150 with EPS of $6")
- Related terms should be actual financial terms that help understand this concept
- Learn more links should be real Investopedia URLs when possible
- Keep the tone informative but approachable

{custom_instructions}

Return ONLY the JSON object, no additional text or formatting."""


def build_glossary_prompt(term: str, custom_instructions: str = "") -> list[dict[str, str]]:
    """
    Build the messages array for a glossary generation request.

    Args:
        term: The financial term to define
        custom_instructions: Optional custom instructions for regeneration

    Returns:
        List of message dicts for the API request
    """
    custom_text = ""
    if custom_instructions:
        custom_text = f"\nAdditional instructions: {custom_instructions}"

    return [
        {"role": "system", "content": GLOSSARY_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": GLOSSARY_USER_PROMPT.format(
                term=term,
                custom_instructions=custom_text,
            ),
        },
    ]


# Predefined regeneration options
REGEN_OPTIONS = {
    "1": ("More technical", "Use more technical language and include formulas if applicable."),
    "2": (
        "Simpler explanation",
        "Make the explanation even simpler, suitable for complete beginners.",
    ),
    "3": ("Add more examples", "Include 4-5 practical examples instead of 2."),
    "4": (
        "Focus on practical use",
        "Focus on how investors actually use this metric in decision-making.",
    ),
}
