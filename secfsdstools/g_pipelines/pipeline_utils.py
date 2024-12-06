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
        paths_to_concat (List[Path]) :
        target_dir:

    Returns:

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