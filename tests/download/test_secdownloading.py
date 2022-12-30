import os
import re
from unittest.mock import MagicMock

import pytest

from secfsdstools.download.secdownloading import SecZipDownloader
from secfsdstools.utils.downloadutils import UrlDownloader

RE_MATCH_QRTR_FILENAME = '^20\d{2}q[1-4]\.zip$'
RE_MATCH_QRTR_FILENAME_compiled = re.compile(RE_MATCH_QRTR_FILENAME)


@pytest.fixture
def seczipdownloader(tmp_path):
    url_downloader = UrlDownloader()
    zip_dir = tmp_path / 'zipfiles'
    os.makedirs(zip_dir)

    yield SecZipDownloader(zip_dir=str(zip_dir), urldownloader=url_downloader)


def test_get_downloaded(seczipdownloader):
    list_of_zips = seczipdownloader._get_downloaded_list()
    assert len(list_of_zips) == 0

    f = open(seczipdownloader.zip_dir + '/demo.zip', 'w')
    f.close()

    list_of_zips = seczipdownloader._get_downloaded_list()
    assert len(list_of_zips) == 1


def test_get_zips_to_download_from_sec_site(seczipdownloader):
    list_available = seczipdownloader._get_zips_to_download()

    assert len(list_available) > 40  # there have to be more than  quarters for 10 years
    for filename, url in list_available.items():
        assert RE_MATCH_QRTR_FILENAME_compiled.match(filename)

    print(list_available)


def test_download_single_zip_file(seczipdownloader):
    url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/2009q1.zip'
    filename = seczipdownloader.zip_dir + '/2009q1.zip'

    # download the real zip
    seczipdownloader._download_zip(url=url, file_path=filename)

    # us the logic of the class to check if there is a zip file with the expected name
    list_of_zips = seczipdownloader._get_downloaded_list()
    assert len(list_of_zips) == 1
    assert list_of_zips[0].endswith('2009q1.zip')


def test_download_missing(seczipdownloader):
    url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/2009q1.zip'

    # download the real zip
    seczipdownloader._download_missing(to_download_entries={'2009q1.zip': url})

    # us the logic of the class to check if there is a zip file with the expected name
    list_of_zips = seczipdownloader._get_downloaded_list()
    assert len(list_of_zips) == 1
    assert list_of_zips[0].endswith('2009q1.zip')


def test_downloading(seczipdownloader):
    seczipdownloader._download_missing = MagicMock()

    seczipdownloader._get_downloaded_list = MagicMock(return_value=['file1'])
    seczipdownloader._get_zips_to_download = MagicMock(return_value={'file1': 'file1', 'file2': 'file2'})

    seczipdownloader.download()

    assert seczipdownloader._download_missing.call_count == 1
    assert seczipdownloader._download_missing.call_args_list[0].args[0] == {'file2': 'file2'}