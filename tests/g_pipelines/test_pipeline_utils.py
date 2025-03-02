import os
from pathlib import Path
from typing import List

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.f_standardize.standardizing import StandardizedBag
from secfsdstools.g_pipelines.pipeline_utils import concat_bags, concat_bags_filebased

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"


def test_concat_bags(tmp_path):
    parts: List[Path] = [
        TESTDATA_PATH / "joined" / "2010q1.zip",
        TESTDATA_PATH / "joined" / "2010q2.zip",
    ]

    concat_bags(paths_to_concat=parts, target_path=tmp_path)

    bag = JoinedDataBag.load(target_path=str(tmp_path))
    # simply check if data were successful stored and loaded
    print(bag.sub_df.shape)
    assert bag.sub_df.shape == (1017, 36)
    assert bag.pre_num_df.shape == (398781, 17)


def test_concat_bags_by_file(tmp_path):
    parts: List[Path] = [
        TESTDATA_PATH / "joined" / "2010q1.zip",
        TESTDATA_PATH / "joined" / "2010q2.zip",
    ]

    concat_bags_filebased(paths_to_concat=parts, target_path=tmp_path)

    bag = JoinedDataBag.load(target_path=str(tmp_path))
    # simply check if data were successful stored and loaded
    print(bag.sub_df.shape)
    assert bag.sub_df.shape == (1017, 36)
    assert bag.pre_num_df.shape == (398781, 17)


def test_concat_bags_by_file_drop_duplicates(tmp_path):
    parts: List[Path] = [
        TESTDATA_PATH / "joined" / "2010q1.zip",
        TESTDATA_PATH / "joined" / "2010q2.zip",
    ]

    concat_bags_filebased(paths_to_concat=parts, target_path=tmp_path, drop_duplicates_sub_df=True)

    bag = JoinedDataBag.load(target_path=str(tmp_path))
    # simply check if data were successful stored and loaded
    print(bag.sub_df.shape)
    assert bag.sub_df.shape == (1017, 36)
    assert bag.pre_num_df.shape == (398781, 17)


def test_concat_bags_by_file_drop_duplicates_2(tmp_path):
    """ read the same file twice, so the sub_df should be different"""
    parts: List[Path] = [
        TESTDATA_PATH / "joined" / "2010q1.zip",
        TESTDATA_PATH / "joined" / "2010q1.zip",
    ]

    concat_bags_filebased(paths_to_concat=parts, target_path=tmp_path, drop_duplicates_sub_df=True)

    bag = JoinedDataBag.load(target_path=str(tmp_path))
    # simply check if data were successful stored and loaded
    print(bag.sub_df.shape)
    assert bag.sub_df.shape == (495, 36)
    assert bag.pre_num_df.shape == (475432, 17)


def test_concat_standardized_bags(tmp_path):
    """ read the same file twice, so the sub_df should be different"""
    parts: List[Path] = [
        TESTDATA_PATH / "is_standardized",
        TESTDATA_PATH / "is_standardized_2",
    ]

    concat_bags(paths_to_concat=parts, target_path=tmp_path)

    bag = StandardizedBag.load(target_path=str(tmp_path))
    # simply check if data were successful stored and loaded
    print(bag.result_df.shape)
    assert bag.result_df.shape == (327, 36)
