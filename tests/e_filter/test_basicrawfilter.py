import os

from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, \
    ReportPeriodAndPreviousPeriodRawFilter, TagRawFilter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_BAG_1 = f'{CURRENT_DIR}/../_testdata/parquet/quarter/2010q1.zip'
PATH_TO_BAG_2 = f'{CURRENT_DIR}/../_testdata/parquet/quarter/2010q2.zip'


def test_filter_ReportPeriodRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    filter = ReportPeriodRawFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == bag1.pre_df.shape
    assert filtered_bag.num_df.shape == (58226, 9)

    # we expect only one ddate for every adsh, so the len has to match the len of sub_df
    assert len(filtered_bag.num_df[['adsh', 'ddate']].value_counts()) == len(bag1.sub_df)


def test_filter_ReportPeriodAndPreviousPeriodRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    filter = ReportPeriodAndPreviousPeriodRawFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == bag1.pre_df.shape
    assert filtered_bag.num_df.shape == (113712, 9)

    # we expect two ddate entries for every adsh, so the len has to be twice the len of sub_df
    assert len(filtered_bag.num_df[['adsh', 'ddate']].value_counts()) == 2 * len(bag1.sub_df)


def test_filter_TagRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    filter = TagRawFilter(tags=["Assets", "Liabilities"])

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == (795, 10)
    assert filtered_bag.num_df.shape == (1652, 9)

    assert len(filtered_bag.pre_df.tag.unique()) == 2
    assert len(filtered_bag.num_df.tag.unique()) == 2

def test_concatenation():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    filter1 = ReportPeriodRawFilter()
    filter2 = TagRawFilter(tags=["Assets", "Liabilities"])

    filtered_bag = bag1.filter(filter1).filter(filter2)
    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == (795, 10)
    assert filtered_bag.num_df.shape == (820, 9)

    # using index operator
    filtered_bag = bag1[filter1][filter2]
    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == (795, 10)
    assert filtered_bag.num_df.shape == (820, 9)
