from decimal import Decimal

import pytest

from sawa.domain.volume import MAX_VOLUME, decimal_volume, sum_volumes


def test_sum_retains_small_fractions_near_the_integer_limit():
    values = [Decimal("9223372036854775806.000000000001"), Decimal("0.000000000002")]
    assert sum_volumes(values) == Decimal("9223372036854775806.000000000003")


def test_large_integer_is_not_rounded_through_float():
    assert decimal_volume(MAX_VOLUME) == Decimal(MAX_VOLUME)
    with pytest.raises(ValueError):
        decimal_volume(MAX_VOLUME + 1)


def test_provider_decimal_string_is_explicit_and_finite():
    assert decimal_volume("0.000001", allow_string=True) == Decimal("0.000001")
    with pytest.raises(ValueError):
        decimal_volume("0.000001")
