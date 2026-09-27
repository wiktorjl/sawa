"""Narrow, source-reviewed identity continuity; never a general CIK allowlist.

These records establish issuer continuity only. Successor conversions also
require the exact persisted split ledger entry; metadata corrections do not
invent a conversion. All price-basis and coverage checks still apply.
Additions require primary-source review.
"""

from dataclasses import dataclass
from datetime import date
from fractions import Fraction


@dataclass(frozen=True)
class ReviewedIssuerSuccessor:
    ticker: str
    old_cik: str
    current_cik: str
    old_composite_figi: str
    current_composite_figi: str
    old_share_class_figi: str
    current_share_class_figi: str
    execution_date: date
    share_ratio: Fraction  # new shares / old shares
    primary_sources: tuple[str, ...]


# Reviewed 2026-09-27. Columbia's Maryland successor replaced the Delaware
# holding company; each existing public share became 2.20 new shares, with the
# same symbol. FIGIs below were verified against dated provider identities.
REVIEWED_ISSUER_SUCCESSORS = (
    ReviewedIssuerSuccessor(
        ticker="CLBK",
        old_cik="1723596",
        current_cik="2115119",
        old_composite_figi="BBG003222R31",
        current_composite_figi="BBG023TM45W0",
        old_share_class_figi="BBG003222R95",
        current_share_class_figi="BBG023TM45X9",
        execution_date=date(2026, 7, 21),
        share_ratio=Fraction(11, 5),
        primary_sources=(
            "https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2026-510",
            "https://www.sec.gov/Archives/edgar/data/2115119/"
            "000119312526309602/d80374dex991.htm",
        ),
    ),
    # Reviewed 2026-09-27. Real's shares consolidated 10:1 after the
    # 2026-08-24 close, then exchanged 1:1 for the new parent's shares.
    # The first split-adjusted trading date (and ledger date) is 2026-08-25.
    # The SEC filing explicitly preserves Real's REAX trading history.
    ReviewedIssuerSuccessor(
        ticker="REAX",
        old_cik="1862461",
        current_cik="2136387",
        old_composite_figi="BBG00VNNPD72",
        current_composite_figi="BBG024M90BY3",
        old_share_class_figi="BBG00KRN3NQ3",
        current_share_class_figi="BBG024M90C26",
        execution_date=date(2026, 8, 25),
        share_ratio=Fraction(1, 10),
        primary_sources=(
            "https://www.sec.gov/Archives/edgar/data/1862461/"
            "000110465926100333/tm2623609d5_6k.htm",
            "https://www.sec.gov/Archives/edgar/data/1862461/"
            "000110465926100333/tm2623609d5_ex99-1.htm",
        ),
    ),
)


@dataclass(frozen=True)
class ReviewedIdentityCorrection:
    ticker: str
    old_cik: str
    current_cik: str
    old_composite_figi: str
    current_composite_figi: str
    share_class_figi: str  # Must match on both sides, not merely one snapshot.
    primary_sources: tuple[str, ...]


# Reviewed 2026-09-27. Both funds already belonged to ProShares Trust II
# (CIK 1415311) in 2021. The provider incorrectly attributed those historical
# records to ProShares Trust (1174610). Pin each observed composite change and
# the unchanged share class; this does not authorize other ProShares funds or
# a general "same share class" exception for conflicting issuer metadata.
REVIEWED_IDENTITY_CORRECTIONS = (
    ReviewedIdentityCorrection(
        ticker="SCO",
        old_cik="1174610",
        current_cik="1415311",
        old_composite_figi="BBG00JVR1SF5",
        current_composite_figi="BBG000CSZTC9",
        share_class_figi="BBG001T0D1Z0",
        primary_sources=(
            "https://www.sec.gov/Archives/edgar/data/1415311/"
            "000119312520120010/d922850d8k.htm",
            "https://www.sec.gov/Archives/edgar/data/1415311/"
            "000119312524260981/d895377d8k.htm",
        ),
    ),
    ReviewedIdentityCorrection(
        ticker="UVXY",
        old_cik="1174610",
        current_cik="1415311",
        old_composite_figi="BBG00DGQ93D6",
        current_composite_figi="BBG0024QY1Y6",
        share_class_figi="BBG0024QY2P4",
        primary_sources=(
            "https://www.sec.gov/Archives/edgar/data/1415311/"
            "000119312521157711/d345771d8k.htm",
            "https://www.sec.gov/Archives/edgar/data/1415311/"
            "000119312524260981/d895377d8k.htm",
        ),
    ),
)
