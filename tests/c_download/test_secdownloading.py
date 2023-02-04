import os
import re
from typing import List, Tuple
from unittest.mock import MagicMock

import pytest

from secfsdstools.a_utils.downloadutils import UrlDownloader
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


def test_calculate_missing_zips(seczipdownloader):
    seczipdownloader._get_downloaded_zips = MagicMock(return_value=['file1'])
    seczipdownloader._get_available_zips = MagicMock(return_value=[('file1', 'file1'), ('file2', 'file2')])

    missing: List[Tuple[str, str]] = seczipdownloader._calculate_missing_zips()
    assert len(missing) == 1
    assert missing == [('file2', 'file2')]
