import os
from typing import List

import numpy as np
from secfsdstools.a_utils.fileutils import (
    concat_parquet_files,
    get_filenames_in_directory,
    read_content_from_zip,
    read_df_from_file_in_zip,
    write_content_to_zip,
)

CURRENT_DIR, CURRENT_FILE = os.path.split(__file__)


def test_write_content_to_zip():
    testdata = 'bla bla bla'
    file_name = 'testzipfile'
    written_file = None
    try:
        written_file = write_content_to_zip(testdata, file_name)

        read_testdata = read_content_from_zip(file_name)

        assert testdata == read_testdata

    finally:
        if written_file:
            os.remove(written_file)


def test_read_df_from_file_in_zip():
    zip_file = CURRENT_DIR + '/../_testdata/zip/2009q3.zip'
    file = 'sub.txt'
    no_options_df = read_df_from_file_in_zip(zip_file=zip_file, file_to_extract=file)

    assert len(no_options_df) == 435
    assert len(no_options_df.columns) == 36
    assert no_options_df['cik'].dtype == np.int64

    # test reading with a certain type
    cik_as_str_df = read_df_from_file_in_zip(zip_file=zip_file, file_to_extract=file,
                                             dtype={'cik': str})
    assert len(cik_as_str_df) == 435
    assert len(cik_as_str_df.columns) == 36
    assert cik_as_str_df['cik'].dtype == object

    # test reading only certain columns
    cik_as_str_df = read_df_from_file_in_zip(zip_file=zip_file, file_to_extract=file,
                                             usecols=['adsh', 'cik'])
    assert len(cik_as_str_df) == 435
    assert len(cik_as_str_df.columns) == 2


def test_get_filenames_in_directory(tmp_path):
    list_of_zips = get_filenames_in_directory(os.path.join(tmp_path, '*.zip'))
    assert len(list_of_zips) == 0

    with open(tmp_path / 'demo.zip', 'w', encoding='utf-8') as f:
        pass

    list_of_zips = get_filenames_in_directory(os.path.join(tmp_path, '*.zip'))
    assert len(list_of_zips) == 1
    assert list_of_zips[0] == 'demo.zip'


def test_file_merge(tmp_path):
    import pyarrow.parquet as pq # pylint: disable=import-outside-toplevel

    file_q1 = CURRENT_DIR + "/../_testdata/parquet_new/quarter/2010q1.zip/sub.txt.parquet"
    file_q2 = CURRENT_DIR + "/../_testdata/parquet_new/quarter/2010q2.zip/sub.txt.parquet"
    file_q3 = CURRENT_DIR + "/../_testdata/parquet_new/quarter/2010q3.zip/sub.txt.parquet"
    file_q4 = CURRENT_DIR + "/../_testdata/parquet_new/quarter/2010q4.zip/sub.txt.parquet"

    input_files: List[str] = [file_q1, file_q2, file_q3, file_q4]

    total_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in input_files)

    print(total_rows)

    output_path = tmp_path / "sub.txt.parquet"

    concat_parquet_files(input_files=input_files, output_file=str(output_path))

    output_rows = pq.ParquetFile(output_path).metadata.num_rows

    assert total_rows == output_rows
