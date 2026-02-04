"""SIC code to GICS sector mapping."""

from typing import NamedTuple


class SectorInfo(NamedTuple):
    """Sector classification info."""

    gics_sector: str
    subsector: str


# SIC code ranges and descriptions mapped to GICS sectors
SIC_TO_GICS = {
    # Technology
    "3570-3579": ("Technology", "Computers & Peripherals"),
    "3571": ("Technology", "Electronic Computers"),
    "3576": ("Technology", "Computer Communications Equipment"),
    "3577": ("Technology", "Computer Peripheral Equipment"),
    "3600-3699": ("Technology", "Electronic Equipment & Components"),
    "3620-3629": ("Technology", "Electrical Equipment"),
    "3661-3679": ("Technology", "Semiconductors & Equipment"),
    "7370-7379": ("Technology", "Software"),
    "7371": ("Technology", "Software - Application"),
    "7372": ("Technology", "Software - Infrastructure"),
    "7373": ("Technology", "IT Services"),
    "7374": ("Technology", "Data Processing Services"),
    "8000-8099": ("Technology", "IT Consulting"),
    # Healthcare
    "2833-2836": ("Healthcare", "Pharmaceuticals"),
    "2834": ("Healthcare", "Pharmaceutical Preparations"),
    "3841-3845": ("Healthcare", "Medical Devices"),
    "3842": ("Healthcare", "Surgical & Medical Instruments"),
    "3844": ("Healthcare", "X-Ray Apparatus"),
    "8000-8099": ("Healthcare", "Healthcare Services"),
    "8060-8069": ("Healthcare", "Hospitals"),
    "8070-8079": ("Healthcare", "Medical Laboratories"),
    "8090-8099": ("Healthcare", "Healthcare Facilities"),
    # Financials
    "6000-6099": ("Financials", "Banks"),
    "6020-6029": ("Financials", "Commercial Banks"),
    "6035-6036": ("Financials", "Savings Institutions"),
    "6200-6299": ("Financials", "Investment Services"),
    "6282": ("Financials", "Investment Advice"),
    "6311": ("Financials", "Life Insurance"),
    "6321": ("Financials", "Accident & Health Insurance"),
    "6331": ("Financials", "Fire & Casualty Insurance"),
    "6500-6553": ("Financials", "Real Estate Investment Trusts"),
    "6700-6799": ("Financials", "Holding Companies"),
    # Consumer Discretionary
    "2300-2399": ("Consumer Discretionary", "Apparel & Textiles"),
    "2510-2599": ("Consumer Discretionary", "Household Furnishings"),
    "3711": ("Consumer Discretionary", "Motor Vehicles"),
    "3714": ("Consumer Discretionary", "Motor Vehicle Parts"),
    "5000-5099": ("Consumer Discretionary", "Wholesale - Durable Goods"),
    "5200-5299": ("Consumer Discretionary", "Retail - Building Materials"),
    "5311": ("Consumer Discretionary", "Department Stores"),
    "5331": ("Consumer Discretionary", "Variety Stores"),
    "5400-5499": ("Consumer Discretionary", "Food Stores"),
    "5500-5599": ("Consumer Discretionary", "Automotive Retailers"),
    "5600-5699": ("Consumer Discretionary", "Apparel Stores"),
    "5700-5799": ("Consumer Discretionary", "Home Furniture Stores"),
    "5900-5999": ("Consumer Discretionary", "Miscellaneous Retail"),
    "5961": ("Consumer Discretionary", "Catalog & Mail-Order"),
    "7800-7899": ("Consumer Discretionary", "Entertainment"),
    "7900-7999": ("Consumer Discretionary", "Amusement & Recreation"),
    "7996": ("Consumer Discretionary", "Amusement Parks"),
    # Consumer Staples
    "2000-2099": ("Consumer Staples", "Food Products"),
    "2080-2089": ("Consumer Staples", "Beverages"),
    "2100-2199": ("Consumer Staples", "Tobacco"),
    "2844": ("Consumer Staples", "Perfumes & Cosmetics"),
    "5140-5149": ("Consumer Staples", "Groceries & Related Products"),
    "5400-5411": ("Consumer Staples", "Grocery Stores"),
    # Industrials
    "1500-1599": ("Industrials", "Construction"),
    "3310-3399": ("Industrials", "Steel & Metal Products"),
    "3440-3499": ("Industrials", "Fabricated Metal Products"),
    "3500-3569": ("Industrials", "Machinery"),
    "3585": ("Industrials", "Air Conditioning & Heating Equipment"),
    "3590-3599": ("Industrials", "Industrial Machinery"),
    "3700-3799": ("Industrials", "Transportation Equipment"),
    "3720-3729": ("Industrials", "Aircraft & Parts"),
    "3730-3739": ("Industrials", "Ship & Boat Building"),
    "4000-4099": ("Industrials", "Railroad Transportation"),
    "4200-4299": ("Industrials", "Trucking & Warehousing"),
    "4400-4499": ("Industrials", "Water Transportation"),
    "4500-4599": ("Industrials", "Air Transportation"),
    "4700-4799": ("Industrials", "Transportation Services"),
    "5000-5199": ("Industrials", "Wholesale Trade"),
    "7300-7399": ("Industrials", "Business Services"),
    "7350-7359": ("Industrials", "Equipment Rental"),
    "8700-8799": ("Industrials", "Engineering Services"),
    # Energy
    "1300-1399": ("Energy", "Oil & Gas Exploration"),
    "1311": ("Energy", "Crude Petroleum & Natural Gas"),
    "1381": ("Energy", "Drilling Oil & Gas Wells"),
    "2900-2999": ("Energy", "Petroleum Refining"),
    "2911": ("Energy", "Petroleum Refining"),
    # Materials
    "1000-1099": ("Materials", "Metal Mining"),
    "2600-2699": ("Materials", "Paper & Forest Products"),
    "2800-2829": ("Materials", "Chemicals"),
    "2812": ("Materials", "Alkalies & Chlorine"),
    "2821": ("Materials", "Plastics Materials"),
    "3200-3299": ("Materials", "Stone, Clay & Glass Products"),
    "3300-3399": ("Materials", "Primary Metal Industries"),
    # Utilities
    "4900-4949": ("Utilities", "Electric Services"),
    "4911": ("Utilities", "Electric Services"),
    "4922": ("Utilities", "Natural Gas Transmission"),
    "4923": ("Utilities", "Natural Gas Distribution"),
    "4924": ("Utilities", "Natural Gas Distribution"),
    "4931": ("Utilities", "Electric & Other Services Combined"),
    # Communication Services
    "2711": ("Communication Services", "Newspapers"),
    "2720-2729": ("Communication Services", "Periodicals"),
    "2731": ("Communication Services", "Books Publishing"),
    "4800-4899": ("Communication Services", "Telecommunications"),
    "4812": ("Communication Services", "Radiotelephone Communications"),
    "4813": ("Communication Services", "Telephone Communications"),
    "4833": ("Communication Services", "Television Broadcasting"),
    "4841": ("Communication Services", "Cable TV"),
    "7810-7819": ("Communication Services", "Motion Picture Production"),
    "7830-7839": ("Communication Services", "Motion Picture Theaters"),
}

# Text patterns for matching SIC descriptions
SIC_DESCRIPTION_PATTERNS = {
    # Technology keywords
    "computer": "Technology",
    "software": "Technology",
    "semiconductor": "Technology",
    "electronic": "Technology",
    "data processing": "Technology",
    "internet": "Technology",
    "information": "Technology",
    "programming": "Technology",
    # Healthcare keywords
    "pharmaceutical": "Healthcare",
    "medical": "Healthcare",
    "hospital": "Healthcare",
    "health": "Healthcare",
    "biotechnology": "Healthcare",
    "surgical": "Healthcare",
    "diagnostic": "Healthcare",
    # Financial keywords
    "bank": "Financials",
    "insurance": "Financials",
    "investment": "Financials",
    "financial": "Financials",
    "securities": "Financials",
    "credit": "Financials",
    "trust": "Financials",
    # Consumer Discretionary
    "retail": "Consumer Discretionary",
    "restaurant": "Consumer Discretionary",
    "hotel": "Consumer Discretionary",
    "automobile": "Consumer Discretionary",
    "apparel": "Consumer Discretionary",
    "entertainment": "Consumer Discretionary",
    # Consumer Staples
    "food": "Consumer Staples",
    "beverage": "Consumer Staples",
    "tobacco": "Consumer Staples",
    "household products": "Consumer Staples",
    # Industrials
    "machinery": "Industrials",
    "aerospace": "Industrials",
    "defense": "Industrials",
    "construction": "Industrials",
    "transportation": "Industrials",
    "engineering": "Industrials",
    # Energy
    "oil": "Energy",
    "gas": "Energy",
    "petroleum": "Energy",
    "energy": "Energy",
    # Materials
    "chemical": "Materials",
    "mining": "Materials",
    "metal": "Materials",
    "paper": "Materials",
    # Utilities
    "electric": "Utilities",
    "utility": "Utilities",
    "water": "Utilities",
    # Communication Services
    "telecommunication": "Communication Services",
    "broadcasting": "Communication Services",
    "media": "Communication Services",
    "publishing": "Communication Services",
    "cable": "Communication Services",
}


def map_sic_to_gics(sic_code: str | None, sic_description: str | None) -> SectorInfo:
    """
    Map SIC code and description to GICS sector.

    Args:
        sic_code: SIC code (e.g., "3571")
        sic_description: SIC description (e.g., "ELECTRONIC COMPUTERS")

    Returns:
        SectorInfo with GICS sector and subsector
    """
    # Default fallback
    default = SectorInfo("Other", "Unclassified")

    # Try exact SIC code match
    if sic_code:
        if sic_code in SIC_TO_GICS:
            gics, subsector = SIC_TO_GICS[sic_code]
            return SectorInfo(gics, subsector)

        # Try range match
        try:
            code_int = int(sic_code)
            for key, (gics, subsector) in SIC_TO_GICS.items():
                if "-" in key:
                    start, end = key.split("-")
                    if int(start) <= code_int <= int(end):
                        return SectorInfo(gics, subsector)
        except (ValueError, TypeError):
            pass

    # Try description pattern match
    if sic_description:
        desc_lower = sic_description.lower()
        for pattern, sector in SIC_DESCRIPTION_PATTERNS.items():
            if pattern in desc_lower:
                # Use description as subsector
                subsector = sic_description.title()
                return SectorInfo(sector, subsector)

    return default
