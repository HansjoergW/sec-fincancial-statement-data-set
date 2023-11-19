import pandas as pd
import pytest

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.standardizing import Standardizer


@pytest.fixture
def empty_instance() -> Standardizer:
    return Standardizer(
        pre_rule_tree=RuleGroup(prefix="pre", rules=[]),
        main_rule_tree=RuleGroup(prefix="main", rules=[]),
        post_rule_tree=RuleGroup(prefix="post", rules=[]),
        validation_rules=[],
        final_tags=[],
        filter_for_main_statement=False
    )


@pytest.fixture
def sample_dataframe_duplications():
    # Create a sample DataFrame for testing with potential duplicates
    data = {
        'adsh': ['A1', 'A2', 'A3', 'A1'],
        'coreg': ['C1', 'C2', 'C3', 'C1'],
        'report': ['R1', 'R2', 'R3', 'R1'],
        'tag': ['T1', 'T2', 'T3', 'T1'],
        'uom': ['U1', 'U2', 'U3', 'U1'],
        'version': ['V1', 'V2', 'V3', 'V1'],
        'ddate': ['D1', 'D2', 'D3', 'D1'],
        'value': [10, 20, 30, 10],
    }
    return pd.DataFrame(data).copy()


def test_preprocess_deduplicate(empty_instance, sample_dataframe_duplications):
    # Test the _preprocess_deduplicate method
    result_df = empty_instance._preprocess_deduplicate(sample_dataframe_duplications)

    # Check that the duplicates are logged correctly
    assert empty_instance.preprocess_dupliate_log_df.shape == (1, 7)
    assert 'adsh' in empty_instance.preprocess_dupliate_log_df.columns
    assert 'coreg' in empty_instance.preprocess_dupliate_log_df.columns
    assert 'report' in empty_instance.preprocess_dupliate_log_df.columns
    assert 'tag' in empty_instance.preprocess_dupliate_log_df.columns
    assert 'uom' in empty_instance.preprocess_dupliate_log_df.columns
    assert 'version' in empty_instance.preprocess_dupliate_log_df.columns
    assert 'ddate' in empty_instance.preprocess_dupliate_log_df.columns

    # Check that duplicates are removed from the result DataFrame
    assert result_df.shape == (3, 8)  # One duplicate row removed
    assert result_df['adsh'].tolist() == ['A1', 'A2', 'A3']
    assert result_df['coreg'].tolist() == ['C1', 'C2', 'C3']
    assert result_df['report'].tolist() == ['R1', 'R2', 'R3']
    assert result_df['tag'].tolist() == ['T1', 'T2', 'T3']
    assert result_df['uom'].tolist() == ['U1', 'U2', 'U3']
    assert result_df['version'].tolist() == ['V1', 'V2', 'V3']
    assert result_df['ddate'].tolist() == ['D1', 'D2', 'D3']
    assert result_df['value'].tolist() == [10, 20, 30]
