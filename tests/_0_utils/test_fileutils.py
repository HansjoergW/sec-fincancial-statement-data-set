import os

from secfsdstools._0_utils.fileutils import write_content_to_zip, read_content_from_zip


def test_write_content_to_zip():
    testdata = "bla bla bla"
    file_name = "testzipfile"
    written_file = None
    try:
        written_file = write_content_to_zip(testdata, file_name)

        read_testdata = read_content_from_zip(file_name)

        assert testdata == read_testdata

    finally:
        if written_file:
            os.remove(written_file)
