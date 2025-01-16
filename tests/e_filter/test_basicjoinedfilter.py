import os

import pytest

from secfsdstools.d_container.databagmodel import JoinedDataBag, RawDataBag
from secfsdstools.e_filter.joinedfiltering import ReportPeriodJoinedFilter, AdshJoinedFilter, \
    ReportPeriodAndPreviousPeriodJoinedFilter, TagJoinedFilter, MainCoregJoinedFilter, \
    StmtJoinedFilter, \
    OfficialTagsOnlyJoinedFilter, USDOnlyJoinedFilter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_BAG_1 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2010q1.zip'

APPLE_10Q_2010Q1 = '0001193125-10-012085'


@pytest.fixture
def bag1() -> JoinedDataBag:
    raw_bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    # fix coreg as it would be loaded by the collectors
    raw_bag1.num_df.loc[raw_bag1.num_df.coreg.isna(), 'coreg'] = ''
    return raw_bag1.join()


def test_filter_StmtsJoinedFilter(bag1):
    filter = StmtJoinedFilter(stmts=['BS'])

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_num_df.shape == (54898, 17)

    pre_stmts = filtered_bag.pre_num_df.stmt.unique()
    assert len(pre_stmts) == 1
    assert pre_stmts[0] == 'BS'


def test_filter_AdshJoinedFilter(bag1):
    filter = AdshJoinedFilter([APPLE_10Q_2010Q1])

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == (1, 36)
    assert filtered_bag.pre_num_df.shape == (154, 17)

    pre_num_adshs = filtered_bag.pre_num_df.adsh.unique()
    assert len(pre_num_adshs) == 1
    assert pre_num_adshs[0] == APPLE_10Q_2010Q1


def test_filter_ReportPeriodJoinedFilter(bag1):
    filter = ReportPeriodJoinedFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_num_df.shape == (84075, 17)

    # we expect only one ddate for every adsh, so the len has to match the len of sub_df
    assert len(filtered_bag.pre_num_df[['adsh', 'ddate']].value_counts()) == len(bag1.sub_df)


def test_filter_ReportPeriodAndPreviousPeriodJoinedFilter(bag1):
    filter = ReportPeriodAndPreviousPeriodJoinedFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_num_df.shape == (165501, 17)

    # we expect two ddate entries for every adsh, so the len has to be twice the len of sub_df
    assert len(filtered_bag.pre_num_df[['adsh', 'ddate']].value_counts()) == 2 * len(bag1.sub_df)


def test_filter_MainCoregRawFilter(bag1):
    filter = MainCoregJoinedFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_num_df.shape == (230074, 17)

    coregs = filtered_bag.pre_num_df.coreg.unique()
    assert len(coregs) == 1
    assert coregs[0] == ""


def test_filter_TagJoinedFilter(bag1):
    filter = TagJoinedFilter(tags=["Assets", "Liabilities"])

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_num_df.shape == (1718, 17)

    assert len(filtered_bag.pre_num_df.tag.unique()) == 2


def test_filter_OfficialTagsOnlyRawFilter(bag1):
    filter = OfficialTagsOnlyJoinedFilter()

    filtered_bag = bag1.filter(filter)

    nr_of_tags_before_filter = bag1.pre_num_df.tag.unique()
    nr_of_tags_after_filter = filtered_bag.pre_num_df.tag.unique()

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert len(nr_of_tags_before_filter) > len(nr_of_tags_after_filter)


def test_concatenation(bag1):
    filter1 = ReportPeriodJoinedFilter()
    filter2 = TagJoinedFilter(tags=["Assets", "Liabilities"])

    filtered_bag = bag1.filter(filter1).filter(filter2)
    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_num_df.shape == (853, 17)

    # using index operator
    filtered_bag = bag1[filter1][filter2]
    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_num_df.shape == (853, 17)


def test_USDOnlyFilter(bag1):
    filter = USDOnlyJoinedFilter()
    filtered_bag = bag1.filter(filter)

    assert bag1.pre_num_df.shape == (237716, 17)
    assert filtered_bag.pre_num_df.shape == (235027, 17)
