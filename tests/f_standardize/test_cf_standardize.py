import os

import numpy as np
import pandas as pd
import pytest

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.joinedfiltering import AdshJoinedFilter
from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
from secfsdstools.u_usecases.bulk_loading import default_postloadfilter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_PARQUET_2021_Q1 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2021q1.zip'

APPLE_10Q_2021Q1 = '0000320193-21-000010'


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


def test_real_values(joined_bag):
    standardizer = CashFlowStandardizer()

    filterd_bag = joined_bag[AdshJoinedFilter(adshs=[APPLE_10Q_2021Q1])]

    result: pd.DataFrame = standardizer.present(filterd_bag)

    # flatten result to series
    cf_series = result.loc[0]
    print(cf_series)

    assert cf_series.adsh == "0000320193-21-000010"
    assert cf_series.cik == 320193
    assert cf_series["name"] == "APPLE INC"
    assert cf_series.form == "10-Q"
    assert cf_series.fye == "0930"
    assert cf_series.fy == 2021.0
    assert cf_series.fp == "Q1"
    assert cf_series.filed == 20210128
    assert cf_series.report == 7
    assert cf_series.ddate == 20201231
    assert cf_series.qtrs == 1
    assert cf_series.NetCashProvidedByUsedInOperatingActivitiesContinuingOperations == 38763000000.0
    assert cf_series.NetCashProvidedByUsedInFinancingActivitiesContinuingOperations == -32249000000.0
    assert cf_series.NetCashProvidedByUsedInInvestingActivitiesContinuingOperations == -8584000000.0
    assert cf_series.NetCashProvidedByUsedInOperatingActivities == 38763000000.0
    assert cf_series.NetCashProvidedByUsedInFinancingActivities == -32249000000.0
    assert cf_series.NetCashProvidedByUsedInInvestingActivities == -8584000000.0
    assert cf_series.CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations == 0.0
    assert cf_series.CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations == 0.0
    assert cf_series.CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations == 0.0
    assert cf_series.EffectOfExchangeRateFinal == 0.0
    assert cf_series.CashPeriodIncreaseDecreaseIncludingExRateEffectFinal == -2070000000.0
    assert cf_series.CashAndCashEquivalentsEndOfPeriod == 37719000000.0
    assert cf_series.DepreciationDepletionAndAmortization == 2666000000.0
    assert cf_series.DeferredIncomeTaxExpenseBenefit == -58000000.0
    assert cf_series.ShareBasedCompensation == 2020000000.0
    assert cf_series.IncreaseDecreaseInAccountsPayable == 21670000000.0
    assert np.isnan(cf_series.IncreaseDecreaseInAccruedLiabilities)
    assert cf_series.InterestPaidNet == 619000000.0
    assert cf_series.IncomeTaxesPaidNet == 1787000000.0
    assert cf_series.PaymentsToAcquirePropertyPlantAndEquipment == -3500000000.0
    assert np.isnan(cf_series.ProceedsFromSaleOfPropertyPlantAndEquipment)
    assert np.isnan(cf_series.PaymentsToAcquireInvestments)
    assert cf_series.ProceedsFromSaleOfInvestments == 25177000000.0
    assert cf_series.PaymentsToAcquireBusinessesNetOfCashAcquired == -9000000.0
    assert np.isnan(cf_series.ProceedsFromDivestitureOfBusinessesNetOfCashDivested)
    assert np.isnan(cf_series.PaymentsToAcquireIntangibleAssets)
    assert np.isnan(cf_series.ProceedsFromSaleOfIntangibleAssets)
    assert cf_series.ProceedsFromIssuanceOfCommonStock == 0.0
    assert np.isnan(cf_series.ProceedsFromStockOptionsExercised)
    assert cf_series.PaymentsForRepurchaseOfCommonStock == -24775000000.0
    assert np.isnan(cf_series.ProceedsFromIssuanceOfDebt)
    assert np.isnan(cf_series.RepaymentsOfDebt)
    assert cf_series.PaymentsOfDividends == -3613000000.0
