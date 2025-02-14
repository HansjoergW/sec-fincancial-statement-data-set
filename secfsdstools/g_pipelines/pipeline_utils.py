"""
Util functions used in pipeline tasks
"""
from pathlib import Path
from typing import List

import pandas as pd

from secfsdstools.a_utils.constants import SUB_TXT, NUM_TXT, PRE_TXT, PRE_NUM_TXT
from secfsdstools.a_utils.fileutils import concat_parquet_files
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag, is_rawbag_path, \
    is_joinedbag_path


def concat_bags(paths_to_concat: List[Path], target_path: Path):
    """
    Concatenates all the Bags in paths_to_concatenate by using the provided bag_type
    into the target_dir directory.

    The logic checks for the type of the bag (Raw or Joined) and handles them accordingly.
    Of course, all paths in the paths_to_concat must be of the same type

    Args:
        paths_to_concat (List[Path]) : List with paths to read the datafrome
        target_path (Path) : path to write the concatenated data to

    """
    if len(paths_to_concat) == 0:
        # nothing to do
        return

    if is_rawbag_path(paths_to_concat[0]):
        all_bags = [RawDataBag.load(str(path)) for path in paths_to_concat]

        all_bag: RawDataBag = RawDataBag.concat(all_bags, drop_duplicates_sub_df=True)
        all_bag.save(target_path=str(target_path))
    elif is_joinedbag_path(paths_to_concat[0]):
        all_bags = [JoinedDataBag.load(str(path)) for path in paths_to_concat]

        all_bag: JoinedDataBag = JoinedDataBag.concat(all_bags, drop_duplicates_sub_df=True)
        all_bag.save(target_path=str(target_path))
    else:
        raise ValueError("bag_type must be either raw or joined")


def _concat_bags_file_based_internal(paths_to_concat: List[Path],
                                     target_path: Path,
                                     file_list: List[str],
                                     drop_duplicates_sub_df: bool = False):
    target_path.mkdir(parents=True, exist_ok=True)
    if not drop_duplicates_sub_df:
        file_list.append(SUB_TXT)

    for file_name in file_list:
        target_path_file = str(target_path / f'{file_name}.parquet')
        paths_to_concat_file = [str(p / f'{file_name}.parquet') for p in paths_to_concat]
        concat_parquet_files(paths_to_concat_file, target_path_file)

    # if we have to drop the duplicates, we need to read the data for the sub into memory
    if drop_duplicates_sub_df:
        sub_dfs: List[pd.DataFrame] = []
        for path_to_concat in paths_to_concat:
            sub_dfs.append(pd.read_parquet(path_to_concat / f'{SUB_TXT}.parquet'))

        sub_df = pd.concat(sub_dfs, ignore_index=True)
        sub_df.drop_duplicates(inplace=True)
        sub_df.to_parquet(target_path / f'{SUB_TXT}.parquet')


def concat_bags_file_based(paths_to_concat: List[Path],
                           target_path: Path,
                           drop_duplicates_sub_df: bool = False):
    """
    Concatenates all the Bags in paths_to_concatenate by using the provided bag_type
    into the target_dir directory.

    The logic checks for the type of the bag (Raw or Joined) and handles them accordingly.
    Of course, all paths in the paths_to_concat must be of the same type

    Args:
        paths_to_concat (List[Path]) : List with paths to read the datafrome
        target_path (Path) : path to write the concatenated data to
        drop_duplicates_sub_df (bool, False) : if the final sub_df should be check for duplicates

    Returns:
    """
    if len(paths_to_concat) == 0:
        # nothing to do
        return

    if is_rawbag_path(paths_to_concat[0]):
        file_list = [NUM_TXT, PRE_TXT]
        _concat_bags_file_based_internal(
            paths_to_concat=paths_to_concat,
            target_path=target_path,
            file_list=file_list,
            drop_duplicates_sub_df=drop_duplicates_sub_df
        )
    elif is_joinedbag_path(paths_to_concat[0]):
        file_list = [PRE_NUM_TXT]
        _concat_bags_file_based_internal(
            paths_to_concat=paths_to_concat,
            target_path=target_path,
            file_list=file_list,
            drop_duplicates_sub_df=drop_duplicates_sub_df
        )
    else:
        raise ValueError("bag_type must be either raw or joined")
