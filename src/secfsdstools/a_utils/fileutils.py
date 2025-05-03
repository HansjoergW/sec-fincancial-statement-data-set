"""
helper utils handling compressed files.
"""
import glob
import logging
import os
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow.parquet as pq

from secfsdstools.a_utils.constants import PA_SCHEMA_MAP

LOGGER = logging.getLogger(__name__)


def concat_parquet_files(input_files: List[str], output_file: str):
    """
    Merges multiple Parquet files with identical columns into a single Parquet file.
    Empty or non-existing files are ignored to optimize memory usage.

    Args:
        input_files (list of str): List of Parquet file paths to merge.
        output_file (str): Path to the output merged Parquet file.
    """

    file_type = os.path.basename(output_file)
    file_type = file_type.replace(".parquet", "")
    schema = PA_SCHEMA_MAP[file_type]

    writer = None  # Writer is created only when a non-empty file is found

    for file in input_files:
        if not os.path.exists(file) or os.path.getsize(file) == 0:
            print(f"Skipping empty file: {file}")
            continue  # Ignore empty files

        # Read Parquet file
        table = pq.read_table(file, columns=schema.names)

        # Ensure the table conforms to the fixed schema
        table = table.cast(schema)

        if writer is None:
            # Create writer with predefined schema
            writer = pq.ParquetWriter(output_file, schema)

        writer.write_table(table)

    if writer:
        writer.close()
    else:
        logging.info("only non-empty files were provided - skipping creation of output file")


def check_dir(target_path: str):
    """
    checks if the path exists and if it is empty.
    if it doesn't exist, it is created.
    """
    target_p = Path(target_path)

    if not target_p.exists():
        target_p.mkdir(parents=True, exist_ok=True)

    if len(os.listdir(target_path)) > 0:
        raise ValueError(f"the target_path {target_path} is not empty")


def get_filenames_in_directory(filter_string: str) -> List[str]:
    """
    returns a list with files matching the pathfilter.
    the pathfilter can also contain a folder structure.

    Returns:
        List[str]: list files in the directory
    """
    zip_list: List[str] = glob.glob(filter_string)
    return [os.path.basename(x) for x in zip_list]


def get_directories_in_directory(directory: str) -> List[str]:
    """
    returns a list with the subdirectory in a directory.

    Returns:
        List[str]: list subdirectories in the directory
    """
    if not os.path.exists(directory):
        return []

    subdirectories: List[str] = [
        entry.name for entry in os.scandir(directory) if entry.is_dir()
    ]
    return subdirectories


def read_df_from_file_in_zip(zip_file: str, file_to_extract: str,
                             dtype: Optional[Dict[str, object]] = None,
                             usecols: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
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
                           dtype=dtype, usecols=usecols, **kwargs)


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
    zip_filename = f"{filename}.zip"
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
    with zipfile.ZipFile(f"{filename}.zip", mode="r") as zf_fp:
        file = Path(filename).name
        return zf_fp.read(file).decode("utf-8")
