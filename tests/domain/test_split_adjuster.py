"""SplitAdjuster re-bases as-traded bars exactly as Polygon's adjusted series does."""

from datetime import date
from decimal import Decimal
from fractions import Fraction

import pytest

from sawa.domain.corporate_actions import SplitAdjuster, StockSplit

NVDA_SPLITS = [
    StockSplit(ticker="NVDA", execution_date=date(2024, 6, 10), split_from=1, split_to=10),
    StockSplit(ticker="NVDA", execution_date=date(2021, 7, 20), split_from=1, split_to=4),
]


def _bar(**overrides: object) -> dict[str, object]:
    bar: dict[str, object] = {
        "ticker": "NVDA",
        "date": "2021-02-17",
        "open": "606.84",
        "high": "608.9407",
        "low": "591.2",
        "close": "596.24",
        "volume": "6761910",
    }
    bar.update(overrides)
    return bar


def test_factor_compounds_every_split_executing_after_the_bar() -> None:
    adjuster = SplitAdjuster(NVDA_SPLITS)
    assert adjuster.factor("NVDA", date(2021, 2, 17)) == Fraction(40)
    # The bar dated on the execution date already trades on the new basis.
    assert adjuster.factor("NVDA", date(2021, 7, 20)) == Fraction(10)
    assert adjuster.factor("NVDA", date(2024, 6, 7)) == Fraction(10)
    assert adjuster.factor("NVDA", date(2024, 6, 10)) == Fraction(1)
    assert adjuster.factor("nvda", date(2020, 1, 1)) == Fraction(40)
    assert adjuster.factor("AAPL", date(2020, 1, 1)) == Fraction(1)


def test_adjust_row_matches_the_provider_basis_for_an_as_traded_nvda_bar() -> None:
    """The flat-file 2021-02-17 bar that sat at 40x its neighbours in stock_prices."""
    adjuster = SplitAdjuster(NVDA_SPLITS)
    raw = _bar()
    adjusted = adjuster.adjust_row(raw)

    assert adjusted["open"] == Decimal("15.17100000")
    assert adjusted["high"] == Decimal("15.22351750")
    assert adjusted["low"] == Decimal("14.78000000")
    assert adjusted["close"] == Decimal("14.90600000")
    assert adjusted["volume"] == 270_476_400
    assert adjusted["ticker"] == "NVDA"
    assert adjusted["date"] == "2021-02-17"
    # The caller's row is not mutated.
    assert raw["close"] == "596.24"


def test_adjust_row_leaves_post_split_and_unknown_bars_alone() -> None:
    adjuster = SplitAdjuster(NVDA_SPLITS)
    post_split = _bar(
        date=date(2024, 6, 10),
        open="120.37",
        high="195.95",
        low="117.01",
        close="121.79",
        volume=314157461,
    )
    assert adjuster.adjust_row(post_split) == post_split
    other = _bar(ticker="AAPL")
    assert adjuster.adjust_row(other) == other


def test_reverse_split_multiplies_prices_and_rounds_volume_half_up() -> None:
    adjuster = SplitAdjuster(
        [StockSplit(ticker="ADTX", execution_date=date(2024, 1, 2), split_from=40, split_to=1)]
    )
    base = _bar(ticker="ADTX", date="2023-12-29", open="0.5", high="0.6", low="0.4", close="0.55")
    adjusted = adjuster.adjust_row({**base, "volume": "1001"})
    assert adjusted["close"] == Decimal("22.00000000")
    assert adjusted["low"] == Decimal("16.00000000")
    assert adjusted["volume"] == 25  # 25.025 rounds down
    assert adjuster.adjust_row({**base, "volume": "1020"})["volume"] == 26  # 25.5 rounds up


def test_from_rows_accepts_database_tuples_and_counts_splits() -> None:
    adjuster = SplitAdjuster.from_rows(
        [
            ("nvda", date(2024, 6, 10), 1, 10),
            ("NVDA", "2021-07-20", 1, 4),
            ("TSLA", date(2022, 8, 25), 1, 3),
        ]
    )
    assert len(adjuster) == 3
    assert adjuster.tickers == frozenset({"NVDA", "TSLA"})
    assert adjuster.factor("NVDA", date(2021, 1, 4)) == Fraction(40)
    assert adjuster.factor("TSLA", date(2022, 8, 24)) == Fraction(3)
    assert adjuster.factor("TSLA", date(2022, 8, 25)) == Fraction(1)


def test_empty_registry_is_a_no_op() -> None:
    adjuster = SplitAdjuster([])
    assert len(adjuster) == 0
    assert adjuster.adjust_row(_bar()) == _bar()


@pytest.mark.parametrize(
    "broken",
    [
        _bar(date="not-a-date"),
        _bar(ticker=None),
        _bar(open=None),
        _bar(close="nan"),
        _bar(volume=True),
        _bar(high="abc"),
    ],
)
def test_adjust_row_rejects_unusable_input(broken: dict[str, object]) -> None:
    adjuster = SplitAdjuster(NVDA_SPLITS)
    with pytest.raises(ValueError):
        adjuster.adjust_row(broken)


def test_adjusted_price_must_stay_storable() -> None:
    adjuster = SplitAdjuster(
        [
            StockSplit(
                ticker="XXXX",
                execution_date=date(2024, 1, 2),
                split_from=2_000_000_000,
                split_to=1,
            )
        ]
    )
    with pytest.raises(ValueError, match="storable price range"):
        adjuster.adjust_row(_bar(ticker="XXXX", date="2023-12-29", high="10000000"))


def test_deeply_compounded_reverse_splits_are_rejected_as_unstorable_not_as_overflow() -> None:
    """ADTX-class history: seven reverse splits push an as-traded price past 28 digits."""
    adjuster = SplitAdjuster(
        [
            StockSplit(
                ticker="ADTX",
                execution_date=date(2024, 1, 2 + i),
                split_from=1_000_000,
                split_to=1,
            )
            for i in range(4)
        ]
    )
    with pytest.raises(ValueError, match="storable price range"):
        adjuster.adjust_row(_bar(ticker="ADTX", date="2023-12-29", open="35617600000"))
