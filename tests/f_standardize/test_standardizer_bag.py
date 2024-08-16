import pytest
import os

from secfsdstools.f_standardize.standardizing import StandardizedBag

CURRENT_DIR, _ = os.path.split(__file__)

@pytest.fixture
def sample_bag1() -> StandardizedBag:
    yield StandardizedBag.load(CURRENT_DIR + "/../_testdata/is_standardized")

@pytest.fixture
def sample_bag2() -> StandardizedBag:
    yield StandardizedBag.load(CURRENT_DIR + "/../_testdata/is_standardized_2")


def test_concatenate(sample_bag1, sample_bag2):
    print(sample_bag2.result_df.shape)
    print(sample_bag1.result_df.shape)
    result = StandardizedBag.concat([sample_bag2, sample_bag1])

    # todo: checks

    print(result.result_df.shape)



