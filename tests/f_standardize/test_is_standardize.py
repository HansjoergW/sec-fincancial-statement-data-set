import os

import pandas as pd
import pytest

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.joinedfiltering import AdshJoinedFilter
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer
from secfsdstools.u_usecases.bulk_loading import default_postloadfilter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_PARQUET_2021_Q1 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2021q1.zip'

APPLE_10Q_2021Q1 = '0000320193-21-000010'


@pytest.fixture
def joined_bag() -> JoinedDataBag:
    collector = ZipCollector(datapaths=[PATH_TO_PARQUET_2021_Q1],
                             forms_filter=["10-K", "10-Q"],
                             stmt_filter=["IS"], post_load_filter=default_postloadfilter)

    joined_bag: JoinedDataBag = collector.collect().join()
    return joined_bag


def test_standardizing(joined_bag):
    standardizer = IncomeStatementStandardizer()

    print("number of loaded reports: ", len(joined_bag.sub_df))
    assert len(joined_bag.sub_df) == 5465

    result = standardizer.present(joined_bag)
    assert len(result) > 5400  # we expect that most reports could be processed

    standardize_bag = standardizer.get_standardize_bag()

    prepivt_rules_log_df = standardize_bag.applied_prepivot_rules_log_df

    # check applied_prepivot_rules_log_df
    # .. check DeDuplication
    dedup_entries = prepivt_rules_log_df[prepivt_rules_log_df.id.str.endswith('DeDup')]
    assert len(dedup_entries) == 619

    # check applied_rules_log_df
    assert len(result) == len(standardize_bag.applied_rules_log_df)

    # check applied_rules_sum_s
    # .. we expect that a significant number of rules were applied
    assert standardize_bag.applied_rules_sum_s.sum() > 50000

    # check validation_overview_df
    validation_overview_df = standardize_bag.validation_overview_df
    assert validation_overview_df.loc[0].RevCogGrossCheck_cat_pct > 92.0
    assert validation_overview_df.loc[0].GrossOpexpOpil_cat_pct > 80.0
    assert validation_overview_df.loc[0].ContIncTax_cat_pct > 93.0
    assert validation_overview_df.loc[0].ProfitLoss_cat_pct > 90.0
    assert validation_overview_df.loc[0].NetIncomeLoss_cat_pct > 95.0

    # check result_df
    assert (standardize_bag.result_df.columns.to_list() == ['adsh', 'cik', 'name', 'form', 'fye',
                                                            'fy', 'fp', 'date', 'filed', 'coreg',
                                                            'report', 'ddate', 'qtrs'] +
            standardizer.final_tags +
            ['RevCogGrossCheck_error',
             'RevCogGrossCheck_cat',
             'GrossOpexpOpil_error',
             'GrossOpexpOpil_cat',
             'ContIncTax_error',
             'ContIncTax_cat',
             'ProfitLoss_error',
             'ProfitLoss_cat',
             'NetIncomeLoss_error',
             'NetIncomeLoss_cat',
             'EPS_error',
             'EPS_cat'])


def test_real_values(joined_bag):
    standardizer = IncomeStatementStandardizer()

    filterd_bag = joined_bag[AdshJoinedFilter(adshs=[APPLE_10Q_2021Q1])]

    result: pd.DataFrame = standardizer.present(filterd_bag)

    # flatten result to series
    is_series = result.loc[0]
    print(is_series)

    assert is_series.adsh == "0000320193-21-000010"
    assert is_series.cik == 320193
    assert is_series["name"] == "APPLE INC"
    assert is_series.form == "10-Q"
    assert is_series.fye == "0930"
    assert is_series.fy == 2021.0
    assert is_series.fp == "Q1"
    assert is_series.filed == 20210128
    assert is_series.ddate == 20201231
    assert is_series.qtrs == 1
    assert is_series.Revenues == 111439000000.0
    assert is_series.CostOfRevenue == 67111000000.0
    assert is_series.GrossProfit == 44328000000.0
    assert is_series.OperatingExpenses == 10794000000.0
    assert is_series.OperatingIncomeLoss == 33534000000.0
    assert is_series.IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit == 33579000000.0
    assert is_series.AllIncomeTaxExpenseBenefit == 4824000000.0
    assert is_series.IncomeLossFromContinuingOperations == 28755000000.0
    assert is_series.IncomeLossFromDiscontinuedOperationsNetOfTax == 0.0
    assert is_series.ProfitLoss == 28755000000.0
    assert is_series.NetIncomeLossAttributableToNoncontrollingInterest == 0.0
    assert is_series.NetIncomeLoss == 28755000000.0
    assert is_series.OutstandingShares == 16935119000.0
    assert is_series.EarningsPerShare == 1.7
