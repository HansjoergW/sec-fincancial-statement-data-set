import os

import pandas as pd
import pytest

from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag
from secfsdstools.e_filter.rawfiltering import AdshRawFilter, ReportPeriodRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_BAG_1 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2010q1.zip'
PATH_TO_BAG_21 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2021q1.zip'

APPLE_10Q_2010Q1 = '0001193125-10-012085'
APPLE_10Q_2021Q1 = '0000320193-21-000010'


@pytest.fixture()
def bag1():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    # fix coreg as it would be loaded by the collectors
    bag1.num_df.loc[bag1.num_df.coreg.isna(), 'coreg'] = ''
    bag1.num_df.loc[bag1.num_df.segments.isna(), 'segments'] = ''

    return bag1


@pytest.fixture()
def bag21():
    bag21: RawDataBag = RawDataBag.load(PATH_TO_BAG_21)
    # fix coreg as it would be loaded by the collectors
    bag21.num_df.loc[bag21.num_df.coreg.isna(), 'coreg'] = ''
    bag21.num_df.loc[bag21.num_df.segments.isna(), 'segments'] = ''

    return bag21


def test_StandardStatementPresenter(bag1):
    joined_bag: JoinedDataBag = \
        bag1[AdshRawFilter([APPLE_10Q_2010Q1])][ReportPeriodRawFilter()].join()

    presentation: pd.DataFrame = joined_bag.present(StandardStatementPresenter())

    assert presentation.shape == (74, 13)


def test_StandardStatementPresenter_not_flatten(bag1):
    joined_bag: JoinedDataBag = \
        bag1[AdshRawFilter([APPLE_10Q_2010Q1])][ReportPeriodRawFilter()].join()

    presentation: pd.DataFrame = joined_bag.present(StandardStatementPresenter(flatten_index=False))

    assert presentation.shape == (74, 2)


def test_StandardStatementPresenter_form_col(bag1):
    joined_bag: JoinedDataBag = \
        bag1[AdshRawFilter([APPLE_10Q_2010Q1])][ReportPeriodRawFilter()].join()

    presentation: pd.DataFrame = \
        joined_bag.present(StandardStatementPresenter(flatten_index=True, add_form_column=True))

    assert presentation.shape == (74, 14)


def test_StandardStatementPresenter_with_segments(bag21):
    joined_bag: JoinedDataBag = \
        bag21[AdshRawFilter([APPLE_10Q_2021Q1])][ReportPeriodRawFilter()].join()

    presentation: pd.DataFrame = joined_bag.present(StandardStatementPresenter(show_segments=True))

    assert presentation.shape == (192, 13)


def test_real_values(bag21):
    " Compare the presneted data for BS against real reported data. "

    joined_bag: JoinedDataBag = \
        bag21[AdshRawFilter([APPLE_10Q_2021Q1])][ReportPeriodRawFilter()].join()

    presentation: pd.DataFrame = joined_bag.present(StandardStatementPresenter(show_segments=False))
    sub_ser = joined_bag.sub_df.iloc[0]

    # just if we want to look up the original
    url = 'https://www.sec.gov/Archives/edgar/data/' + str(sub_ser['cik']) + '/' + \
          sub_ser['adsh'].replace('-', '') + '/' + sub_ser['adsh'] + '-index.htm'
    print(url)

    bs = presentation[(presentation.stmt == 'BS') & (presentation.report == 4)][
        ['tag', 'qrtrs_0/20201231']]
    bs_series = bs.set_index('tag')['qrtrs_0/20201231'].squeeze()

    assert bs_series.CashAndCashEquivalentsAtCarryingValue == 3.601000e+10
    assert bs_series.MarketableSecuritiesCurrent == 4.081600e+10
    assert bs_series.AccountsReceivableNetCurrent == 2.710100e+10
    assert bs_series.InventoryNet == 4.973000e+09
    assert bs_series.NontradeReceivablesCurrent == 3.151900e+10
    assert bs_series.OtherAssetsCurrent == 1.368700e+10
    assert bs_series.AssetsCurrent == 1.541060e+11
    assert bs_series.MarketableSecuritiesNoncurrent == 1.187450e+11
    assert bs_series.PropertyPlantAndEquipmentNet == 3.793300e+10
    assert bs_series.OtherAssetsNoncurrent == 4.327000e+10
    assert bs_series.AssetsNoncurrent == 1.999480e+11
    assert bs_series.Assets == 3.540540e+11
    assert bs_series.AccountsPayableCurrent == 6.384600e+10
    assert bs_series.OtherLiabilitiesCurrent == 4.850400e+10
    assert bs_series.ContractWithCustomerLiabilityCurrent == 7.395000e+09
    assert bs_series.CommercialPaper == 5.000000e+09
    assert bs_series.LongTermDebtCurrent == 7.762000e+09
    assert bs_series.LiabilitiesCurrent == 1.325070e+11
    assert bs_series.LongTermDebtNoncurrent == 9.928100e+10
    assert bs_series.OtherLiabilitiesNoncurrent == 5.604200e+10
    assert bs_series.LiabilitiesNoncurrent == 1.553230e+11
    assert bs_series.Liabilities == 2.878300e+11
    assert bs_series.CommonStocksIncludingAdditionalPaidInCapital == 5.174400e+10
    assert bs_series.RetainedEarningsAccumulatedDeficit == 1.430100e+10
    assert bs_series.AccumulatedOtherComprehensiveIncomeLossNetOfTax == 1.790000e+08
    assert bs_series.StockholdersEquity == 6.622400e+10
    assert bs_series.LiabilitiesAndStockholdersEquity == 3.540540e+11
