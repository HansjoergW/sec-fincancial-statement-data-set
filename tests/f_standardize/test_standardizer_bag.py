import os
from pathlib import Path

import pytest

from secfsdstools.f_standardize.standardizing import StandardizedBag

CURRENT_DIR, _ = os.path.split(__file__)


@pytest.fixture
def sample_bag1() -> StandardizedBag:
    yield StandardizedBag.load(f"{CURRENT_DIR}/../_testdata/is_standardized")


@pytest.fixture
def sample_bag2() -> StandardizedBag:
    yield StandardizedBag.load(f"{CURRENT_DIR}/../_testdata/is_standardized_2")


def test_concatenate(sample_bag1, sample_bag2):
    result = StandardizedBag.concat([sample_bag2, sample_bag1])

    # do some basic checks
    assert result.result_df.shape == (327, 36)
    assert result.applied_prepivot_rules_log_df.shape == (3780, 9)
    assert result.applied_rules_log_df.shape == (327, 168)
    assert result.applied_rules_sum_s.shape == (164,)
    assert result.stats_df.shape == (12, 14)

    print(result.result_df.shape)


def test_is_standardizedbag():
    assert StandardizedBag.is_standardizebag_path(
        Path(CURRENT_DIR) / ".." / "_testdata" / "is_standardized")

    assert not StandardizedBag.is_standardizebag_path(
        Path(CURRENT_DIR) / ".." / "_testdata" / "joined" / "2010q1.zip")
