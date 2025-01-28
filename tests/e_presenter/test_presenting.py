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


def test_StandardStatementPresenter_with_segments(bag21):
    joined_bag: JoinedDataBag = \
        bag21[AdshRawFilter([APPLE_10Q_2021Q1])][ReportPeriodRawFilter()].join()

    presentation: pd.DataFrame = joined_bag.present(StandardStatementPresenter(show_segments=True))

    assert presentation.shape == (192, 13)


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
