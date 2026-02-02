"""Prompt templates for AI-generated content."""

GLOSSARY_SYSTEM_PROMPT = (
    "You are a financial education expert who explains complex financial concepts "
    "clearly. Your goal is to help investors understand financial terminology in a "
    "practical, accessible way. Always return valid JSON without any markdown "
    "formatting or code blocks."
)

GLOSSARY_USER_PROMPT = """Generate a comprehensive glossary entry for the financial term: "{term}"

Return a JSON object with this exact structure:
{{
  "official_definition": "A formal, textbook-style definition in 1-2 sentences.",
  "plain_english": "What this actually means in simple, conversational terms. \
Explain like you're talking to a smart friend who isn't a finance expert. \
Make it relatable and easy to understand.",
  "examples": [
    "A practical example with real or realistic numbers that illustrates the concept",
    "Another example showing a different scenario or use case"
  ],
  "related_terms": [
    "Related Term 1", "Related Term 2", "Related Term 3",
    "Related Term 4", "Related Term 5"
  ],
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


# Predefined regeneration options for glossary
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


# Company Overview Prompts

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


def build_company_overview_prompt(
    ticker: str,
    company_name: str,
    sector: str | None = None,
    custom_instructions: str = "",
) -> list[dict[str, str]]:
    """
    Build the messages array for a company overview generation request.

    Args:
        ticker: Stock ticker symbol
        company_name: Full company name
        sector: Industry/sector description
        custom_instructions: Optional custom instructions for regeneration

    Returns:
        List of message dicts for the API request
    """
    custom_text = ""
    if custom_instructions:
        custom_text = f"\nAdditional instructions: {custom_instructions}"

    return [
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


# Predefined regeneration options for company overview
OVERVIEW_REGEN_OPTIONS = {
    "1": ("More bullish focus", "Focus more on growth potential and positive catalysts."),
    "2": ("More bearish focus", "Focus more on risks, challenges, and potential problems."),
    "3": ("Technical deep-dive", "Include more technical and product-specific details."),
    "4": ("Valuation context", "Add valuation and price context to each section."),
}
