import os
import re
from typing import List, Tuple
from unittest.mock import MagicMock, patch

import pytest

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_filenames_in_directory
from secfsdstools.c_download.secdownloading import SecZipDownloader


RE_MATCH_QRTR_FILENAME = '^20\d{2}q[1-4]\.zip$'
RE_MATCH_QRTR_FILENAME_compiled = re.compile(RE_MATCH_QRTR_FILENAME)


@pytest.fixture
def seczipdownloader(tmp_path):
    url_downloader = UrlDownloader()
    zip_dir = tmp_path / 'zipfiles'
    os.makedirs(zip_dir)

    yield SecZipDownloader(zip_dir=str(zip_dir), urldownloader=url_downloader, execute_serial=True)


def test_get_available_zips(seczipdownloader):
    list_available = seczipdownloader._get_available_zips()

    assert len(list_available) > 40  # there have to be more than  quarters for 10 years
    for filename, url in list_available:
        assert RE_MATCH_QRTR_FILENAME_compiled.match(filename)

    print(list_available)


def test_download_single_zip_file(seczipdownloader):
    url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/2009q1.zip'
    filename = '2009q1.zip'

    # download the real zip
    seczipdownloader._download_zip(url=url, file=filename)

    # us the logic of the class to check if there is a zip file with the expected name
    list_of_zips = get_filenames_in_directory(os.path.join(seczipdownloader.zip_dir, '*.zip'))
    assert len(list_of_zips) == 1
    assert list_of_zips[0].endswith('2009q1.zip')


def test_calculate_missing_zips(seczipdownloader):
    with patch("secfsdstools.c_download.secdownloading.get_filenames_in_directory", return_value=['file1']):
        seczipdownloader._get_available_zips = MagicMock(return_value=[('file1', 'file1'), ('file2', 'file2')])

        missing: List[Tuple[str, str]] = seczipdownloader._calculate_missing_zips()
        assert len(missing) == 1
        assert missing == [('file2', 'file2')]


def test_downloading(seczipdownloader):
    seczipdownloader._download_file = MagicMock()

    seczipdownloader._calculate_missing_zips = MagicMock(return_value=[('file2', 'file2')])

    seczipdownloader.download()

    assert seczipdownloader._download_file.call_count == 1

    import platform
    if platform.python_version().startswith("3.7"):
        assert seczipdownloader._download_file.call_args_list[0][0][0]== ('file2', 'file2')
    else:
        assert seczipdownloader._download_file.call_args_list[0].args[0] == ('file2', 'file2')
