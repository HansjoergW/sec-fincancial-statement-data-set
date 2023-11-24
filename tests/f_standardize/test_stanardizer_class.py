import numpy as np
import pandas as pd
import pytest

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_validation_rules import SumValidationRule
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


@pytest.fixture
def sample_dataframe_pivot():
    data = {
        'adsh': ['A1', 'A2', 'A3'],
        'coreg': ['C1', 'C2', 'C3'],
        'report': ['R1', 'R2', 'R3'],
        'tag': ['T1', 'T2', 'T3'],
        'uom': ['U1', 'U2', 'U3'],
        'value': [100, 50, 80],
        'ddate': ['D1', 'D2', 'D3'],
    }
    return pd.DataFrame(data)


def test_preprocess_pivot(empty_instance, sample_dataframe_pivot):
    # Initialize the instance with the sample data
    empty_instance.data_df = sample_dataframe_pivot.copy()

    # Define expected_tags
    expected_tags = {'T1', 'T2', 'T3', 'T4', 'T5'}

    # Test the _preprocess_pivot method
    result_df = empty_instance._preprocess_pivot(empty_instance.data_df, expected_tags)

    # Check the structure of the result DataFrame
    assert result_df.shape == (3, len(empty_instance.identifier_tags) + len(expected_tags))

    # Check the content of the result DataFrame
    expected_columns = empty_instance.identifier_tags + list(expected_tags)
    assert len(set(result_df.columns.tolist()) - set(expected_columns)) == 0

    # Check values in the result DataFrame
    columns_to_test = ['T1', 'T2', 'T3', 'T4', 'T5']

    # define which entries should not be nan
    no_nan_definition = {'T1': 0, 'T2': 1, 'T3': 2}

    # a little cumbersome, since we have to check for nan values
    for column_to_test in columns_to_test:
        data = result_df[column_to_test].tolist()
        for idx in range(len(data)):
            value = data[idx]
            if no_nan_definition.get(column_to_test, 0) == idx:
                assert value is not None
            else:
                assert np.isnan(value)


@pytest.fixture
def sample_dataframe_filter():
    # Create a sample DataFrame for testing
    data = {
        'adsh': ['A1', 'A1', 'A1', 'A2', 'A2', 'A3', 'A3', 'A3'],
        'coreg': ['C1', 'C1', 'C1', 'C2', 'C2', 'C3', 'C3', 'C3'],
        'report': ['1', '2', '3', '1', '2', '1', '2', '3'],
        'T1': [10, np.nan, 30, 40, 50, 60, 70, 80],
        'T2': [20, np.nan, 35, np.nan, 55, 65, 75, 85],
        'T3': [30, 35, np.nan, 50, 60, np.nan, 80, 90],
        'T4': [40, 45, 55, 60, 70, 80, np.nan, 100],
        'T5': [50, 55, 65, 70, 80, 90, 100, 110],
    }
    return pd.DataFrame(data)


def test_preprocess_filter_pivot_for_main_statement(empty_instance, sample_dataframe_filter):
    empty_instance.main_statement_tags = ['T1', 'T2', 'T3', 'T4', 'T5']

    filtered_df = empty_instance._preprocess_filter_pivot_for_main_statement(
        sample_dataframe_filter)

    # there should be one single report for A1, A2, and A3
    assert len(filtered_df) == 3

    # the data was defined in a way, that the selected rows don't have a nan
    assert filtered_df['nan_count'].sum() == 0

    # based on the input data:
    # from A1, report 1 should be selected, from A2, report 2 and from A3, report 3
    assert filtered_df.report.tolist() == ['1', '2', '3']


@pytest.fixture
def sample_dataframe_preprocess():
    data = {
        'adsh': ['A1', 'A2', 'A3'],
        'coreg': ['C1', 'C2', 'C3'],
        'report': ['R1', 'R2', 'R3'],
        'tag': ['T1', 'T2', 'T3'],
        'uom': ['U1', 'U2', 'U3'],
        'value': [100, 50, 80],
        'ddate': ['D1', 'D2', 'D3'],
        'version': ['1', '1', '1'],
        'line': [1, 1, 1],
        'negating': [0, 0, 0]
    }
    return pd.DataFrame(data)


def test_preprocess(empty_instance, sample_dataframe_preprocess):
    empty_instance.all_input_tags = {'T1', 'T2', 'T3'}
    pivoted_df = empty_instance._preprocess(sample_dataframe_preprocess)

    # ensure that the dataframe was pivoted
    assert 'T1' in pivoted_df.columns.tolist()
    assert 'T2' in pivoted_df.columns.tolist()
    assert 'T3' in pivoted_df.columns.tolist()

    # we expect that the applied_rules_log_df was instantiated
    assert empty_instance.applied_rules_log_df.columns.tolist() == \
           ['adsh', 'coreg', 'report', 'ddate', 'uom']

    # ensure that the stats are initialized and that the pre column was added
    assert empty_instance.stats.stats.columns.tolist() == ['pre']


def test_finalize():
    instance = Standardizer(
        pre_rule_tree=RuleGroup(prefix='pre', rules=[]),
        main_rule_tree=RuleGroup(prefix='main', rules=[]),
        post_rule_tree=RuleGroup(prefix='post', rules=[]),
        validation_rules=[SumValidationRule(identifier='V1', sum_tag='T1', summands=['T2', 'T3'])],
        final_tags=['T1', 'T2', 'T3'],
        filter_for_main_statement=False
    )

    data = {
        'adsh': ['A1', 'A2', 'A3'],
        'coreg': ['C1', 'C2', 'C3'],
        'report': ['1', '2', '3'],
        'ddate': ['20221231', '20221231', '20221231'],
        'uom': ['USD', 'USD', 'USD'],
        'T1': [60, 70, 80],
        'T2': [20, 35, 90],
        'T3': [40, 40, -10],
    }

    data_df = pd.DataFrame(data).copy()

    # configure log dataframes and stats
    rules_log_df = data_df[instance.identifier_tags].copy()
    instance.applied_rules_log_df = rules_log_df

    # add pseudo rule
    instance.applied_rules_log_df['RULE1'] = False

    instance.stats.initialize(data_df=data_df, process_step_name="pre")
    instance.stats.add_stats_entry(data_df=data_df, process_step_name="post")

    result = instance._finalize(data_df=data_df)

    assert 'V1_error' in result.columns.tolist()
    assert 'V1_cat' in result.columns.tolist()

    # check if summary was created for RULE1
    assert len(instance.applied_rules_sum_s) == 1
    assert instance.applied_rules_sum_s.loc['RULE1'] == 0

    # check if expected rows are present for stats
    assert instance.stats.stats.columns.tolist() == ['pre', 'pre_rel', 'post', 'post_rel', 'post_gain']
