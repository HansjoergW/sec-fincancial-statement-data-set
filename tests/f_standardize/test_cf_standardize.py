import os

import pytest

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
from secfsdstools.u_usecases.bulk_loading import default_postloadfilter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_PARQUET_2021_Q1 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2021q1.zip'


@pytest.fixture
def joined_bag() -> JoinedDataBag:
    collector = ZipCollector(datapaths=[PATH_TO_PARQUET_2021_Q1],
                             forms_filter=["10-K", "10-Q"],
                             stmt_filter=["CF"], post_load_filter=default_postloadfilter)

    joined_bag: JoinedDataBag = collector.collect().join()
    return joined_bag


def test_standardizing(joined_bag):
    standardizer = CashFlowStandardizer()

    print("number of loaded reports: ", len(joined_bag.sub_df))
    assert len(joined_bag.sub_df) == 5465

    result = standardizer.present(joined_bag)
    assert len(result) > 5200  # we expect that most reports could be processed

    standardize_bag = standardizer.get_standardize_bag()

    prepivt_rules_log_df = standardize_bag.applied_prepivot_rules_log_df

    # check applied_prepivot_rules_log_df
    # .. check DeDuplication
    dedup_entries = prepivt_rules_log_df[prepivt_rules_log_df.id.str.endswith('DeDup')]
    assert len(dedup_entries) == 5760

    # check applied_rules_log_df
    assert len(result) == len(standardize_bag.applied_rules_log_df)

    # check applied_rules_sum_s
    # .. we expect that a significant number of rules were applied
    assert standardize_bag.applied_rules_sum_s.sum() > 100000

    # check validation_overview_df
    validation_overview_df = standardize_bag.validation_overview_df
    assert validation_overview_df.loc[0].BaseOpAct_cat_pct > 99.0
    assert validation_overview_df.loc[0].BaseFinAct_cat_pct > 99.0
    assert validation_overview_df.loc[0].BaseInvAct_cat_pct > 99.0
    assert validation_overview_df.loc[0].NetCashContOp_cat_pct > 97.0
    assert validation_overview_df.loc[0].CashEoP_cat_pct > 98.0

    # check result_df
    assert (standardize_bag.result_df.columns.to_list() == ['adsh', 'cik', 'name', 'form', 'fye',
                                                            'fy', 'fp', 'date', 'filed', 'coreg',
                                                            'report', 'ddate', 'qtrs'] +
            standardizer.final_tags +
            ['BaseOpAct_error',
             'BaseOpAct_cat',
             'BaseFinAct_error',
             'BaseFinAct_cat',
             'BaseInvAct_error',
             'BaseInvAct_cat',
             'NetCashContOp_error',
             'NetCashContOp_cat',
             'CashEoP_error',
             'CashEoP_cat'])
