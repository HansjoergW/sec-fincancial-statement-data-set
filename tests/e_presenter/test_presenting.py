import os

import pandas as pd

from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag
from secfsdstools.e_filter.rawfiltering import AdshRawFilter, ReportPeriodRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_BAG_1 = f'{CURRENT_DIR}/../_testdata/parquet/quarter/2010q1.zip'

APPLE_10Q_2010Q1 = '0001193125-10-012085'


def test_StandardStatementPresenter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    joined_bag: JoinedDataBag = \
        bag1[AdshRawFilter([APPLE_10Q_2010Q1])][ReportPeriodRawFilter()].get_joined_bag()

    presentation: pd.DataFrame = joined_bag.present(StandardStatementPresenter())

    assert presentation.shape == (74, 1)


def test_StandardStatementPresenter_flatten():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    joined_bag: JoinedDataBag = \
        bag1[AdshRawFilter([APPLE_10Q_2010Q1])][ReportPeriodRawFilter()].get_joined_bag()

    presentation: pd.DataFrame = joined_bag.present(StandardStatementPresenter(flatten_index=True))

    assert presentation.shape == (74, 11)


def test_StandardStatementPresenter_form_col():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    joined_bag: JoinedDataBag = \
        bag1[AdshRawFilter([APPLE_10Q_2010Q1])][ReportPeriodRawFilter()].get_joined_bag()

    presentation: pd.DataFrame = \
        joined_bag.present(StandardStatementPresenter(flatten_index=True, add_form_column=True))

    assert presentation.shape == (74, 12)
