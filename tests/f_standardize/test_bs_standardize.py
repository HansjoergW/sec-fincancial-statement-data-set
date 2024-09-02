import os

import pytest

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer
from secfsdstools.u_usecases.bulk_loading import default_postloadfilter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_PARQUET_2021_Q1 = f'{CURRENT_DIR}/../_testdata/parquet/quarter/2021q1.zip'


@pytest.fixture
def joined_bag() -> JoinedDataBag:
    collector = ZipCollector(datapaths=[PATH_TO_PARQUET_2021_Q1],
                             forms_filter=["10-K", "10-Q"],
                             stmt_filter=["BS"], post_load_filter=default_postloadfilter)

    joined_bag: JoinedDataBag = collector.collect().join()
    return joined_bag


def test_standardizing(joined_bag):
    standardizer = BalanceSheetStandardizer()

    print("number of loaded reports: ", len(joined_bag.sub_df))
    assert len(joined_bag.sub_df) == 5470

    result = standardizer.present(joined_bag)
    assert len(result) > 5400  # we expect that most reports could be processed

    standardize_bag = standardizer.get_standardize_bag()

    prepivt_rules_log_df = standardize_bag.applied_prepivot_rules_log_df

    # check applied_prepivot_rules_log_df
    # .. check DeDuplication
    dedup_entries = prepivt_rules_log_df[prepivt_rules_log_df.id.str.endswith('DeDup')]
    assert len(dedup_entries) == 38

    # check applied_rules_log_df
    assert len(result) == len(standardize_bag.applied_rules_log_df)

    # check applied_rules_sum_s
    # .. we expect that a significant number of rules were applied
    assert standardize_bag.applied_rules_sum_s.sum() > 50000

    # check validation_overview_df
    validation_overview_df = standardize_bag.validation_overview_df
    assert validation_overview_df.loc[0].AssetsCheck_cat_pct > 99.5
    assert validation_overview_df.loc[0].LiabilitiesCheck_cat_pct > 95.0
    assert validation_overview_df.loc[0].EquityCheck_cat_pct > 93.0
    assert validation_overview_df.loc[0].AssetsLiaEquCheck_cat_pct > 93.0

    # check result_df
    assert (standardize_bag.result_df.columns.to_list() == ['adsh', 'cik', 'name', 'form', 'fye',
                                                            'fy', 'fp', 'date', 'filed', 'coreg',
                                                            'report', 'ddate', 'qtrs'] +
            standardizer.final_tags +
            ['AssetsCheck_error', 'AssetsCheck_cat', 'LiabilitiesCheck_error',
             'LiabilitiesCheck_cat',
             'EquityCheck_error', 'EquityCheck_cat', 'AssetsLiaEquCheck_error',
             'AssetsLiaEquCheck_cat'])


def test_options_standardizing(joined_bag):
    standardizer = BalanceSheetStandardizer(additional_final_sub_fields=['zipba'])

    result_df = standardizer.present(joined_bag)

    assert (result_df.columns.to_list() == ['adsh', 'cik', 'name', 'form', 'fye',
                                            'fy', 'fp', 'date', 'filed', 'zipba', 'coreg',
                                            'report', 'ddate', 'qtrs'] +
            standardizer.final_tags +
            ['AssetsCheck_error', 'AssetsCheck_cat', 'LiabilitiesCheck_error',
             'LiabilitiesCheck_cat',
             'EquityCheck_error', 'EquityCheck_cat', 'AssetsLiaEquCheck_error',
             'AssetsLiaEquCheck_cat'])
