"""
Util functions used in pipeline tasks
"""
import logging
from pathlib import Path
from typing import List

from secfsdstools.d_container.databagmodel import JoinedDataBag, RawDataBag
from secfsdstools.f_standardize.standardizing import StandardizedBag

LOGGER = logging.getLogger(__name__)


def concat_bags(paths_to_concat: List[Path], target_path: Path,
                drop_duplicates_sub_df: bool = True):
    """
    Concatenates all the Bags in paths_to_concatenate by using the provided bag_type
    into the target_dir directory.

    The logic checks for the type of the bag (Raw or Joined) and handles them accordingly.
    Of course, all paths in the paths_to_concat must be of the same type

    Args:
        paths_to_concat (List[Path]) : List with paths to read the datafrome
        target_path (Path) : path to write the concatenated data to
        drop_duplicates_sub_df (bool, False) : if the final sub_df should be check for duplicates

    """
    if len(paths_to_concat) == 0:
        # nothing to do
        return

    LOGGER.info("Concat in memory - number of paths: %d - target: %s",
                len(paths_to_concat), target_path)

    if RawDataBag.is_rawbag_path(paths_to_concat[0]):
        all_bags = [RawDataBag.load(str(path)) for path in paths_to_concat]

        all_bag: RawDataBag = RawDataBag.concat(all_bags,
                                                drop_duplicates_sub_df=drop_duplicates_sub_df)
        all_bag.save(target_path=str(target_path))

    elif JoinedDataBag.is_joinedbag_path(paths_to_concat[0]):
        all_bags = [JoinedDataBag.load(str(path)) for path in paths_to_concat]

        all_bag: JoinedDataBag = JoinedDataBag.concat(all_bags,
                                                      drop_duplicates_sub_df=drop_duplicates_sub_df)
        all_bag.save(target_path=str(target_path))

    elif StandardizedBag.is_standardizebag_path(paths_to_concat[0]):
        all_bags = [StandardizedBag.load(str(path)) for path in paths_to_concat]
        all_bag: StandardizedBag = StandardizedBag.concat(all_bags)
        all_bag.save(target_path=str(target_path))
    else:
        raise ValueError("bag_type must be either raw, joined, or standardized")


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

    LOGGER.info("Concat on filesystem - number of paths: %d - target: %s",
                len(paths_to_concat), target_path)

    if RawDataBag.is_rawbag_path(paths_to_concat[0]):
        RawDataBag.concat_filebased(paths_to_concat=paths_to_concat,
                                    target_path=target_path,
                                    drop_duplicates_sub_df=drop_duplicates_sub_df)

    elif JoinedDataBag.is_joinedbag_path(paths_to_concat[0]):
        JoinedDataBag.concat_filebased(paths_to_concat=paths_to_concat,
                                       target_path=target_path,
                                       drop_duplicates_sub_df=drop_duplicates_sub_df)
    else:
        raise ValueError("bag_type must be either raw or joined")
