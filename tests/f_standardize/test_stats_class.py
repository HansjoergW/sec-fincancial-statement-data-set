import pandas as pd
import pytest
from secfsdstools.f_standardize.standardizing import Stats


@pytest.fixture
def sample_dataframe():
    # Create a sample DataFrame for testing
    data = {
        'Assets': [100, None, 200, 150],
        'AssetsCurrent': [40, 20, None, 30],
        'AssetsNoncurrent': [60, 30, 50, None],
        'Liabilities': [80, 30, 50, None],
        'LiabilitiesCurrent': [30, None, 20, 10],
        'LiabilitiesNoncurrent': [50, 30, 30, None],
        'OwnerEquity': [20, 20, 20, 20],
        'LiabilitiesAndOwnerEquity': [100, 80, 100, None],
    }
    return pd.DataFrame(data)


@pytest.fixture
def stats_instance(sample_dataframe):
    tags = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent', 'Liabilities', 'LiabilitiesCurrent',
            'LiabilitiesNoncurrent', 'OwnerEquity', 'LiabilitiesAndOwnerEquity']
    stats = Stats(tags)
    stats.initialize(sample_dataframe, process_step_name='preprocessing')
    return stats


def test_initialize_stats(stats_instance):
    assert stats_instance.stats.shape == (8, 1)
    assert stats_instance.stats.columns == ['preprocessing']
    assert stats_instance.stats.iloc[:, 0].tolist() == [1, 1, 1, 1, 1, 1, 0, 1]


def test_add_stats_entry(stats_instance, sample_dataframe):
    stats_instance.add_stats_entry(sample_dataframe, process_step_name='iteration_1')
    assert stats_instance.stats.shape == (8, 2)
    assert stats_instance.stats.columns.tolist() == ['preprocessing', 'iteration_1']
    assert stats_instance.stats.iloc[:, 1].tolist() == [1, 1, 1, 1, 1, 1, 0, 1]


def test_finalize_stats(stats_instance, sample_dataframe):

    data = {
        'Assets': [100, 150, 200, 150],
        'AssetsCurrent': [40, 20, 25, 30],
        'AssetsNoncurrent': [60, 30, 50, 40],
        'Liabilities': [80, 30, 50, None],
        'LiabilitiesCurrent': [30, None, 20, 10],
        'LiabilitiesNoncurrent': [50, 30, 30, None],
        'OwnerEquity': [20, 20, 20, 20],
        'LiabilitiesAndOwnerEquity': [100, 80, 100, None],
    }
    updated_data = pd.DataFrame(data)

    stats_instance.add_stats_entry(updated_data, process_step_name='iteration_1')
    stats_instance.finalize_stats(data_length=len(sample_dataframe))

    assert stats_instance.stats.shape == (8, 5)
    assert 'preprocessing' in stats_instance.stats.columns
    assert 'iteration_1' in stats_instance.stats.columns
    assert 'preprocessing_rel' in stats_instance.stats.columns
    assert 'iteration_1_rel' in stats_instance.stats.columns
    assert 'iteration_1_gain' in stats_instance.stats.columns

    # Test values in 'rel' columns
    assert stats_instance.stats['preprocessing_rel'].tolist() == [0.25, 0.25, 0.25, 0.25, 0.25,
                                                                  0.25, 0.0, 0.25]
    assert stats_instance.stats['iteration_1_rel'].tolist() == [0.0, 0.0, 0.0, 0.25, 0.25, 0.25,
                                                                0.0, 0.25]

    # Test values in 'gain' columns
    assert stats_instance.stats['iteration_1_gain'].tolist() == [0.25, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0,
                                                                 0.0]
