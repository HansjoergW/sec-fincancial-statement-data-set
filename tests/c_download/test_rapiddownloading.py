import json
import os

import pytest

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.c_download.rapiddownloading import RapidZipDownloader


@pytest.fixture
def rapidzipdownloader(tmp_path):
    url_downloader = UrlDownloader()
    zip_dir = tmp_path / 'zipfiles'
    os.makedirs(zip_dir)
    rapid_api_key = os.environ.get('RAPID_API_KEY')

    yield RapidZipDownloader(rapid_api_key=rapid_api_key, zip_dir=str(zip_dir), urldownloader=url_downloader,
                             execute_serial=True)

# todo: es muss geprüft werden, ob basic oder premium account verwendet wird


def test_get_content(rapidzipdownloader):
    # check if the call to the api is working
    content = rapidzipdownloader._get_content()
    parsed_json = json.loads(content)

    assert parsed_json['daily']


def test_get_zip_dir(rapidzipdownloader):
    assert rapidzipdownloader.zip_dir.endswith("daily")


def test_test():
    print(os.environ.get('RAPID_API_KEY'))
