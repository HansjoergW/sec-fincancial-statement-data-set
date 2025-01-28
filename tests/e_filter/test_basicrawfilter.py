import os

from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, AdshRawFilter, \
    ReportPeriodAndPreviousPeriodRawFilter, TagRawFilter, MainCoregRawFilter, StmtRawFilter, \
    OfficialTagsOnlyRawFilter, USDOnlyRawFilter, NoSegmentInfoRawFilter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_BAG_1 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2010q1.zip'

APPLE_10Q_2010Q1 = '0001193125-10-012085'


def test_filter_StmtsRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    filter = StmtRawFilter(stmts=['BS'])

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == (20373, 10)
    assert filtered_bag.num_df.shape == bag1.num_df.shape

    pre_stmts = filtered_bag.pre_df.stmt.unique()
    assert len(pre_stmts) == 1
    assert pre_stmts[0] == 'BS'


def test_filter_AdshRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    filter = AdshRawFilter([APPLE_10Q_2010Q1])

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == (1, 36)
    assert filtered_bag.pre_df.shape == (74, 10)
    assert filtered_bag.num_df.shape == (144, 10)

    pre_adshs = filtered_bag.pre_df.adsh.unique()
    assert len(pre_adshs) == 1
    assert pre_adshs[0] == APPLE_10Q_2010Q1

    num_adshs = filtered_bag.num_df.adsh.unique()
    assert len(num_adshs) == 1
    assert num_adshs[0] == APPLE_10Q_2010Q1


def test_filter_ReportPeriodRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    filter = ReportPeriodRawFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == bag1.pre_df.shape
    assert filtered_bag.num_df.shape == (71894, 10)

    # we expect only one ddate for every adsh, so the len has to match the len of sub_df
    assert len(filtered_bag.num_df[['adsh', 'ddate']].value_counts()) == len(bag1.sub_df)


def test_filter_ReportPeriodAndPreviousPeriodRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    filter = ReportPeriodAndPreviousPeriodRawFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == bag1.pre_df.shape
    assert filtered_bag.num_df.shape == (141280, 10)

    # we expect two ddate entries for every adsh, so the len has to be twice the len of sub_df
    assert len(filtered_bag.num_df[['adsh', 'ddate']].value_counts()) == 2 * len(bag1.sub_df)


def test_filter_MainCoregRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    # fix coreg as it would be loaded by the collectors
    bag1.num_df.loc[bag1.num_df.coreg.isna(), 'coreg'] = ''

    filter = MainCoregRawFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == bag1.pre_df.shape
    assert filtered_bag.num_df.shape == (187881, 10)

    coregs = filtered_bag.num_df.coreg.unique()
    assert len(coregs) == 1
    assert coregs[0] == ""


def test_filter_TagRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    filter = TagRawFilter(tags=["Assets", "Liabilities"])

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == (793, 10)
    assert filtered_bag.num_df.shape == (1710, 10)

    assert len(filtered_bag.pre_df.tag.unique()) == 2
    assert len(filtered_bag.num_df.tag.unique()) == 2


def test_filter_OfficialTagsOnlyRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    filter = OfficialTagsOnlyRawFilter()

    filtered_bag = bag1.filter(filter)

    nr_of_tags_before_filter = bag1.pre_df.tag.unique()
    nr_of_tags_after_filter = filtered_bag.pre_df.tag.unique()

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert len(nr_of_tags_before_filter) > len(nr_of_tags_after_filter)


def test_concatenation():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    filter1 = ReportPeriodRawFilter()
    filter2 = TagRawFilter(tags=["Assets", "Liabilities"])

    filtered_bag = bag1.filter(filter1).filter(filter2)
    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == (793, 10)
    assert filtered_bag.num_df.shape == (849, 10)

    # using index operator
    filtered_bag = bag1[filter1][filter2]
    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == (793, 10)
    assert filtered_bag.num_df.shape == (849, 10)


def test_USDOnlyFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    filter = USDOnlyRawFilter()
    filtered_bag = bag1.filter(filter)

    assert bag1.num_df.shape == (194741, 10)
    assert filtered_bag.num_df.shape == (192629, 10)

def test_filter_NoSegmentInfoRawFilter():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    bag1.num_df.loc[bag1.num_df.segments.isna(), 'segments'] = ''

    filter = NoSegmentInfoRawFilter()

    filtered_bag = filter.filter(bag1)

    assert filtered_bag.sub_df.shape == bag1.sub_df.shape
    assert filtered_bag.pre_df.shape == bag1.pre_df.shape
    assert filtered_bag.num_df.shape == (142929, 10)
