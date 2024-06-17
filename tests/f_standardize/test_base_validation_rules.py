import numpy as np
import pandas as pd
import pandera as pa
import pytest

from secfsdstools.f_standardize.base_validation_rules import SumValidationRule, ValidationRule, \
    IsSetValidationRule


# Test the basic framework
class SimpleValidationRule(ValidationRule):
    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        return df['Value'].notnull()

    def calculate_error(self, df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        return ((df.loc[mask, 'Value'] - 10) / 10).abs()

    def get_description(self) -> str:
        return "Simple Validation Rule"


@pytest.fixture
def sample_dataframe():
    # Create a sample DataFrame for testing
    data = {
        'Value': [10, 10.1, 10.5, 20],
    }
    return pd.DataFrame(data)


def test_simple_validation_rule_categories(sample_dataframe):
    rule = SimpleValidationRule(identifier='test_rule')
    rule.validate(sample_dataframe)

    assert sample_dataframe['test_rule_error'].round(decimals=2).tolist() == [0.0, 0.01, 0.05, 1.0]
    assert sample_dataframe['test_rule_error'].round(decimals=2) \
        .equals(pd.Series([0.0, 0.01, 0.05, 1.0]))


# ----------------------------------------------------------------------------------
# Tests for the SumValidationRule

@pytest.fixture
def sample_dataframe_sum():
    # Create a sample DataFrame for testing
    data = {
        'Assets': [100, 50, 70, 120],
        'AssetsCurrent': [40, 20, 30, 60],
        'AssetsNoncurrent': [60, 30, 40, 60],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sum_validation_rule():
    return SumValidationRule(identifier='test_sum_rule',
                             sum_tag='Assets',
                             summands=['AssetsCurrent', 'AssetsNoncurrent'])


def test_validate_exact_match(sample_dataframe_sum, sum_validation_rule):
    # Test for an exact match
    sum_validation_rule.validate(sample_dataframe_sum)

    assert sample_dataframe_sum['test_sum_rule_cat'].equals(pd.Series([0.0, 0.0, 0.0, 0.0]))
    assert sample_dataframe_sum['test_sum_rule_error'].equals(pd.Series([0.0, 0.0, 0.0, 0.0]))


def test_validate_within_1_percent(sample_dataframe_sum, sum_validation_rule):
    # Test for a match within 1%
    # Modify the sample DataFrame to introduce a small error within 1%
    sample_dataframe_sum['Assets'] = [100.5, 50, 70, 120]

    sum_validation_rule.validate(sample_dataframe_sum)

    assert sample_dataframe_sum['test_sum_rule_cat'].equals(pd.Series([1.0, 0.0, 0.0, 0.0]))
    assert sample_dataframe_sum['test_sum_rule_error'].round(decimals=3).equals(
        pd.Series([0.005, 0.0, 0.0, 0.0]))


def test_validate_within_5_percent(sample_dataframe_sum, sum_validation_rule):
    # Test for a match within 5%
    # Modify the sample DataFrame to introduce a small error within 5%
    sample_dataframe_sum['Assets'] = [105, 50, 70, 120]

    sum_validation_rule.validate(sample_dataframe_sum)

    assert sample_dataframe_sum['test_sum_rule_cat'].equals(pd.Series([5.0, 0.0, 0.0, 0.0]))
    assert sample_dataframe_sum['test_sum_rule_error'].round(decimals=2).equals(
        pd.Series([0.05, 0.0, 0.0, 0.0]))


def test_validate_within_10_percent(sample_dataframe_sum, sum_validation_rule):
    # Test for a match within 10%
    # Modify the sample DataFrame to introduce a small error within 10%
    sample_dataframe_sum['Assets'] = [110, 50, 70, 120]

    sum_validation_rule.validate(sample_dataframe_sum)

    assert sample_dataframe_sum['test_sum_rule_cat'].equals(pd.Series([10.0, 0.0, 0.0, 0.0]))
    assert sample_dataframe_sum['test_sum_rule_error'].round(decimals=1).equals(
        pd.Series([0.1, 0.0, 0.0, 0.0]))


def test_validate_above_10_percent(sample_dataframe_sum, sum_validation_rule):
    # Test for a match above 10%
    # Modify the sample DataFrame to introduce an error above 10%
    sample_dataframe_sum['Assets'] = [130, 50, 70, 120]

    sum_validation_rule.validate(sample_dataframe_sum)

    assert sample_dataframe_sum['test_sum_rule_cat'].equals(pd.Series([100.0, 0.0, 0.0, 0.0]))
    assert sample_dataframe_sum['test_sum_rule_error'].round(decimals=1).equals(
        pd.Series([0.2, 0.0, 0.0, 0.0]))


def test_with_zero_sum(sum_validation_rule):
    data = {
        'Assets': [0.0],
        'AssetsCurrent': [0.0],
        'AssetsNoncurrent': [0.0],
    }
    data_df = pd.DataFrame(data)

    sum_validation_rule.validate(data_df)

    assert data_df['test_sum_rule_cat'].equals(pd.Series([0.0]))
    assert data_df['test_sum_rule_error'].round(decimals=1).equals(pd.Series([0.0]))


# ----------------------------------------------------------------------------------
# Tests for the IsSetValidationRule

@pytest.fixture
def sample_dataframe_isset():
    # Create a sample DataFrame for testing
    data = {
        'Assets': [100, 50, 70, None],
    }
    return pd.DataFrame(data)


@pytest.fixture
def isset_validation_rule():
    return IsSetValidationRule(identifier='test_isset_rule',tag='Assets')


def test_correct_categories(isset_validation_rule, sample_dataframe_isset):

    isset_validation_rule.validate(sample_dataframe_isset)

    assert sample_dataframe_isset['test_isset_rule_cat'].equals(pd.Series([0.0, 0.0, 0.0, 100.0]))
    assert sample_dataframe_isset['test_isset_rule_error'].round(decimals=1).equals(
        pd.Series([0.0, 0.0, 0.0, 1.0]))
