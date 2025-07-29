"""
Util function that are mainly used in the context of automation.
"""
import os
import shutil
from pathlib import Path
from typing import List, Optional

from secfsdstools.a_utils.fileutils import get_directories_in_directory


def delete_temp_folders(root_path: Path, temp_prefix: str = "tmp"):
    """
    Remove any existing folders starting with the tmp_prefix in the root_path
     (these are generally folders containing data of tasks that failed)).

    """
    dirs_in_filter_dir = get_directories_in_directory(str(root_path))

    tmp_dirs = [d for d in dirs_in_filter_dir if d.startswith(temp_prefix)]

    for tmp_dir in tmp_dirs:
        file_path = root_path / tmp_dir
        shutil.rmtree(file_path, ignore_errors=True)


def get_latest_mtime(root_paths: List[Path],
                     skip: Optional[List[str]] = None) -> float:
    """
    Find the latest timestamp at which an element in the folder structure was changed
    Args:
        root_paths: List of root folder

    Returns:
        the latest timestamp of a folder or file within the "folder" as float value.

    """
    if skip is None:
        skip = []

    latest_mtime = 0

    for root_path in root_paths:
        for dirpath, dirnames, filenames in os.walk(root_path):
            # Check the modification timestamp of files
            filenames = list(set(filenames) - set(skip))
            for filename in filenames:
                file_path = Path(dirpath) / filename
                mtime = file_path.stat().st_mtime  # modification timestamp of the file
                latest_mtime = max(latest_mtime, mtime)

            # Check the modification timestamp of subfolders
            for dirname in dirnames:
                dir_path = Path(dirpath) / dirname
                mtime = dir_path.stat().st_mtime  # modification timestamp of the folder
                latest_mtime = max(latest_mtime, mtime)

    return latest_mtime
