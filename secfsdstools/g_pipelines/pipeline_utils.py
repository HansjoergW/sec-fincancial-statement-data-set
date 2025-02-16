"""
Util functions used in pipeline tasks
"""
from pathlib import Path
from typing import List

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


def concat_bags_filebased(paths_to_concat: List[Path],
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
        RawDataBag.concat_filebased(paths_to_concat=paths_to_concat,
                                    target_path=target_path,
                                    drop_duplicates_sub_df=drop_duplicates_sub_df)

    elif is_joinedbag_path(paths_to_concat[0]):
        JoinedDataBag.concat_filebased(paths_to_concat=paths_to_concat,
                                       target_path=target_path,
                                       drop_duplicates_sub_df=drop_duplicates_sub_df)
    else:
        raise ValueError("bag_type must be either raw or joined")
