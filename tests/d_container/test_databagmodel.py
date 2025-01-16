import os
from pathlib import Path

from secfsdstools.d_container.databagmodel import RawDataBag, RawDataBagStats, JoinedDataBag, \
    is_joinedbag_path, is_rawbag_path

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"
PATH_TO_BAG_1 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2010q1.zip'
PATH_TO_BAG_2 = f'{CURRENT_DIR}/../_testdata/parquet_new/quarter/2010q2.zip'


# def test_create_joined():
#     """ creates the joined bags from the zip """
#     from typing import List
#
#     zips: List[str] = ["2010q1.zip", "2010q2.zip", "2010q3.zip", "2010q4.zip"]
#     for zipfile in zips:
#         target_path = TESTDATA_PATH / "joined" / zipfile
#         target_path.mkdir(parents=True, exist_ok=True)
#         RawDataBag.load(str(TESTDATA_PATH / "parquet_new" / "quarter" / zipfile)).join().save(str(TESTDATA_PATH / "joined" / zipfile))


def test_is_rawbag():
    assert is_rawbag_path(TESTDATA_PATH / "parquet_new" / "quarter" / "2010q1.zip")
    assert not is_rawbag_path(TESTDATA_PATH / "joined" / "2010q1.zip")


def test_is_joinedbag():
    assert not is_joinedbag_path(TESTDATA_PATH / "parquet_new" / "quarter" / "2010q1.zip")
    assert is_joinedbag_path(TESTDATA_PATH / "joined" / "2010q1.zip")


def test_load_method():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    assert bag1.num_df.shape == (194741, 10)
    assert bag1.pre_df.shape == (64151, 10)
    assert bag1.sub_df.shape == (495, 36)

    bag2: RawDataBag = RawDataBag.load(PATH_TO_BAG_2)

    assert bag2.num_df.shape == (136044, 10)
    assert bag2.pre_df.shape == (57596, 10)
    assert bag2.sub_df.shape == (522, 36)


def test_concat():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    bag2: RawDataBag = RawDataBag.load(PATH_TO_BAG_2)
    bag_merged = RawDataBag.concat([bag1, bag2])

    assert bag_merged.num_df.shape == (136044 + 194741, 10)
    assert bag_merged.pre_df.shape == (64151 + 57596, 10)
    assert bag_merged.sub_df.shape == (522 + 495, 36)


def test_save(tmp_path):
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    bag1.save(str(tmp_path))

    bag1_load: RawDataBag = RawDataBag.load(str(tmp_path))

    assert bag1_load.sub_df.shape == bag1.sub_df.shape
    assert bag1_load.num_df.shape == bag1.num_df.shape
    assert bag1_load.pre_df.shape == bag1.pre_df.shape


def test_statistics():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    stats: RawDataBagStats = bag1.statistics()

    assert stats.pre_entries == 64151
    assert stats.num_entries == 194741
    assert stats.number_of_reports == 495
    assert len(stats.reports_per_period_date) == 7
    assert len(stats.reports_per_form) == 8


def test_get_joined_bag():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    joined_bag: JoinedDataBag = bag1.join()

    assert joined_bag.sub_df.shape == (495, 36)
    assert joined_bag.pre_num_df.shape == (237716, 17)


def test_load_save_joined_bag(tmp_path):
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    joined_bag: JoinedDataBag = bag1.join()

    joined_bag.save(str(tmp_path))

    joined_bag_load: JoinedDataBag = JoinedDataBag.load(str(tmp_path))

    assert joined_bag.sub_df.shape == joined_bag_load.sub_df.shape
    assert joined_bag.pre_num_df.shape == joined_bag_load.pre_num_df.shape


def test_merge_joined_bag():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    bag2: RawDataBag = RawDataBag.load(PATH_TO_BAG_2)

    joined_bag_1: JoinedDataBag = bag1.join()

    joined_bag_2: JoinedDataBag = bag2.join()

    concatenated: JoinedDataBag = JoinedDataBag.concat([joined_bag_1, joined_bag_2])

    assert concatenated.sub_df.shape == (1017, 36)
    assert concatenated.pre_num_df.shape == (398781, 17)
