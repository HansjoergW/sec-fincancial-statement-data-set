import os
from typing import List, Tuple

import pytest
from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_filenames_in_directory
from secfsdstools.c_download.basedownloading_process import BaseDownloadingProcess


class MyDownloader(BaseDownloadingProcess):
    missing_zips: List[Tuple[str, str]] = []

    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        return self.missing_zips


@pytest.fixture
def basedownloader(tmp_path):
    url_downloader = UrlDownloader(user_agent="abc.xyz@main.com")
    zip_dir = tmp_path / 'zipfiles'
    parquet_dir = tmp_path / 'parquet'
    os.makedirs(zip_dir)

    yield MyDownloader(zip_dir=str(zip_dir), urldownloader=url_downloader, execute_serial=True,
                       parquet_dir=str(parquet_dir))


def test_download_single_zip_file(basedownloader):
    url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/2009q1.zip'
    filename = '2009q1.zip'

    basedownloader.missing_zips = [(filename, url)]

    # create the download tasks
    tasks = basedownloader.calculate_tasks()
    assert len(tasks) == 1

    # actually download the zip
    tasks[0].execute()

    # us the logic of the class to check if there is a zip file with the expected name
    list_of_zips = get_filenames_in_directory(os.path.join(basedownloader.zip_dir, '*.zip'))
    assert len(list_of_zips) == 1
    assert list_of_zips[0].endswith('2009q1.zip')
