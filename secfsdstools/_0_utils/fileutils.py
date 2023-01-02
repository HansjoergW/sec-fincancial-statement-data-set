"""
helper utils condhandling compressed files.
"""

import zipfile
from pathlib import Path

import pandas as pd


def read_df_from_file_in_zip(zip_file: str, file_to_extract: str,
                             dtype=None, usecols=None) -> pd.DataFrame:
    """
    reads the content of a file inside a zip file directly into dataframe
    :param zip_file: the zip file containing the data file
    :param file_to_extract: the file with the data
    :param dtype: column type array or None
    :param usecols: list with all the columns that should be read or None
    :return: the pandas dataframe
    """
    with zipfile.ZipFile(zip_file, "r") as zip_fp:
        file = Path(file_to_extract).name
        return pd.read_csv(zip_fp.open(file), header=0, delimiter="\t",
                           dtype=dtype, usecols=usecols)


def read_content_from_file_in_zip(zip_file: str, file_to_extract: str) -> str:
    """
    reads the text content of a file inside a zip file
    :param zip_file: the zip file containing the data file
    :param file_to_extract: the file with the data
    :return: the content as string
    """
    with zipfile.ZipFile(zip_file, "r") as zip_fp:
        file = Path(file_to_extract).name
        return zip_fp.read(file).decode("utf-8")


def write_content_to_zip(content: str, filename: str) -> str:
    """
    write the content str into the zip file. compression is set to zipfile.ZIP_DEFLATED
    :param content: string
    :param filename: string name of the target zipfile, withouit the ending ".zip"
    :return: written zipfilename
    """
    zip_filename = filename + ".zip"
    with zipfile.ZipFile(zip_filename, mode="w", compression=zipfile.ZIP_DEFLATED) as zf_fp:
        file = Path(filename).name
        zf_fp.writestr(file, content)
    return zip_filename


def read_content_from_zip(filename: str) -> str:
    """
    returns the content of the provided zipfile (ending ".zip)
    :param filename: zipfilename without the ending ".zip"
    :return:
    """
    with zipfile.ZipFile(filename + ".zip", mode="r") as zf_fp:
        file = Path(filename).name
        return zf_fp.read(file).decode("utf-8")
