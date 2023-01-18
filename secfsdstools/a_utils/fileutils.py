"""
helper utils handling compressed files.
"""
import glob
import os
import zipfile
from pathlib import Path
from typing import List, Optional, Dict

import pandas as pd


def get_filenames_in_directory(filter_string: str) -> List[str]:
    """
    returns a list with files matching the filter.
    the filter can also contain a folder structure.

    Returns:
        List[str]: list files in the directory
    """
    zip_list: List[str] = glob.glob(filter_string)
    return [os.path.basename(x) for x in zip_list]


def read_df_from_file_in_zip(zip_file: str, file_to_extract: str,
                             dtype: Optional[Dict[str, object]] = None,
                             usecols: Optional[List[str]] = None) -> pd.DataFrame:
    """
    reads the content of a file inside a zip file directly into dataframe

    Args:
        zip_file (str): the zip file containing the data file
        file_to_extract (str): the file with the data
        dtype (Dict[str, object], optional, None): column type array or None
        usecols (List[str], optional, None): list with all the columns
        that should be read or None
    Returns:
        pd.DataFrame: the pandas dataframe
    """
    with zipfile.ZipFile(zip_file, "r") as zip_fp:
        file = Path(file_to_extract).name
        return pd.read_csv(zip_fp.open(file), header=0, delimiter="\t",
                           dtype=dtype, usecols=usecols)


def read_content_from_file_in_zip(zip_file: str, file_to_extract: str) -> str:
    """
    reads the text content of a file inside a zip file

    Args:
        zip_file (str): the zip file containing the data file
        file_to_extract (str): the file with the data

    Returns:
        str: the content as string
    """
    with zipfile.ZipFile(zip_file, "r") as zip_fp:
        file = Path(file_to_extract).name
        return zip_fp.read(file).decode("utf-8")


def write_content_to_zip(content: str, filename: str) -> str:
    """
    write the content str into the zip file. compression is set to zipfile.ZIP_DEFLATED

    Args:
        content (str): the content that should be written into the file
        filename (str): string name of the target zipfile, without the ending ".zip"

    Returns:
        str: path to the zipfile that was ritten
    """
    zip_filename = filename + ".zip"
    with zipfile.ZipFile(zip_filename, mode="w", compression=zipfile.ZIP_DEFLATED) as zf_fp:
        file = Path(filename).name
        zf_fp.writestr(file, content)
    return zip_filename


def read_content_from_zip(filename: str) -> str:
    """
    returns the content of the provided zipfile (ending ".zip)
    Args:
        filename (str): string name of the target zipfile, without the ending ".zip"

    Returns:
        str: the content of a zipfile
    """
    with zipfile.ZipFile(filename + ".zip", mode="r") as zf_fp:
        file = Path(filename).name
        return zf_fp.read(file).decode("utf-8")
