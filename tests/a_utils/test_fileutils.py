import os

import numpy as np

from secfsdstools.a_utils.fileutils import write_content_to_zip, read_content_from_zip, read_df_from_file_in_zip, get_filenames_in_directory

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
    zip_file = CURRENT_DIR + '/testdata/2009q3.zip'
    file = 'sub.txt'
    no_options_df = read_df_from_file_in_zip(zip_file=zip_file, file_to_extract=file)

    assert len(no_options_df) == 439
    assert len(no_options_df.columns) == 36
    assert no_options_df['cik'].dtype == np.int64

    # test reading with a certain type
    cik_as_str_df = read_df_from_file_in_zip(zip_file=zip_file, file_to_extract=file, dtype={'cik': str})
    assert len(cik_as_str_df) == 439
    assert len(cik_as_str_df.columns) == 36
    assert cik_as_str_df['cik'].dtype == object

    # test reading only certain columns
    cik_as_str_df = read_df_from_file_in_zip(zip_file=zip_file, file_to_extract=file, usecols=['adsh', 'cik'])
    assert len(cik_as_str_df) == 439
    assert len(cik_as_str_df.columns) == 2


def test_get_filenames_in_directory(tmp_path):
    list_of_zips = get_filenames_in_directory(os.path.join(tmp_path, '*.zip'))
    assert len(list_of_zips) == 0

    f = open(tmp_path / 'demo.zip', 'w')
    f.close()

    list_of_zips = get_filenames_in_directory(os.path.join(tmp_path, '*.zip'))
    assert len(list_of_zips) == 1
    assert list_of_zips[0] == 'demo.zip'
