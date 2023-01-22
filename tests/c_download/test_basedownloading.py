import os
from typing import List, Tuple
from unittest.mock import MagicMock

import pytest

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_filenames_in_directory
from secfsdstools.c_download.basedownloading import BaseDownloader


class MyDownloader(BaseDownloader):

    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        return []


@pytest.fixture
def basedownloader(tmp_path):
    url_downloader = UrlDownloader()
    zip_dir = tmp_path / 'zipfiles'
    os.makedirs(zip_dir)

    yield MyDownloader(zip_dir=str(zip_dir), urldownloader=url_downloader, execute_serial=True)


def test_download_single_zip_file(basedownloader):
    url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/2009q1.zip'
    filename = '2009q1.zip'

    # download the real zip
    basedownloader._download_zip(url=url, file=filename)

    # us the logic of the class to check if there is a zip file with the expected name
    list_of_zips = get_filenames_in_directory(os.path.join(basedownloader.zip_dir, '*.zip'))
    assert len(list_of_zips) == 1
    assert list_of_zips[0].endswith('2009q1.zip')


def test_downloading(basedownloader):
    basedownloader._download_file = MagicMock()

    basedownloader._calculate_missing_zips = MagicMock(return_value=[('file2', 'file2')])

    basedownloader.download()

    assert basedownloader._download_file.call_count == 1

    import platform
    if platform.python_version().startswith("3.7"):
        assert basedownloader._download_file.call_args_list[0][0][0] == ('file2', 'file2')
    else:
        assert basedownloader._download_file.call_args_list[0].args[0] == ('file2', 'file2')
