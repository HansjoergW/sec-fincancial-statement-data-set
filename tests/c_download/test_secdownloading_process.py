import os
import re
from typing import List, Tuple
from unittest.mock import MagicMock

import pytest

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.c_download.secdownloading_process import SecDownloadingProcess

RE_MATCH_QRTR_FILENAME = '^20\d{2}q[1-4]\.zip$'
RE_MATCH_QRTR_FILENAME_compiled = re.compile(RE_MATCH_QRTR_FILENAME)


@pytest.fixture
def seczipdownloader(tmp_path) -> SecDownloadingProcess:
    url_downloader = UrlDownloader(user_agent="abc.xyz@main.com")
    zip_dir = tmp_path / 'zipfiles'
    parquet_dir = tmp_path / 'parquet'
    os.makedirs(zip_dir)

    yield SecDownloadingProcess(zip_dir=str(zip_dir),
                                urldownloader=url_downloader,
                                execute_serial=True,
                                parquet_root_dir=str(parquet_dir))


def test_get_available_zips(seczipdownloader):
    list_available = seczipdownloader._get_available_zips()

    assert len(list_available) > 40  # there have to be more than  quarters for 10 years
    for filename, url in list_available:
        assert RE_MATCH_QRTR_FILENAME_compiled.match(filename)

    print(list_available)


def test_calculate_missing_zips(seczipdownloader):
    # test missing available zip which was not transformed to parquet
    seczipdownloader._get_available_zips = MagicMock(
        return_value=[('file1', 'file1'), ('file2', 'file2')])
    seczipdownloader._get_downloaded_zips = MagicMock(return_value=['file1'])
    seczipdownloader._get_transformed_parquet = MagicMock(return_value=['file1'])

    missing: List[Tuple[str, str]] = seczipdownloader._calculate_missing_zips()
    assert len(missing) == 1
    # file2 needs to be downloaded
    assert missing == [('file2', 'file2')]


def test_calculate_missing_zips_transformed_not_downloaded(seczipdownloader):
    seczipdownloader._get_available_zips = MagicMock(
        return_value=[('file1', 'file1'), ('file2', 'file2')])
    seczipdownloader._get_downloaded_zips = MagicMock(return_value=[])
    seczipdownloader._get_transformed_parquet = MagicMock(return_value=['file1'])

    missing: List[Tuple[str, str]] = seczipdownloader._calculate_missing_zips()
    assert len(missing) == 1
    # only file2 needs to be downloaded, even if file1 is not present as zip since it was
    # already transformed
    assert missing == [('file2', 'file2')]
