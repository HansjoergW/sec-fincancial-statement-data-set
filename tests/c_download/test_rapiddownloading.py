import json
import os
from unittest.mock import MagicMock, patch

import pytest

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.rapiddownloadutils import RapidUrlBuilder
from secfsdstools.c_download.rapiddownloading import RapidZipDownloader


@pytest.fixture
def rapidzipdownloader(tmp_path):
    url_downloader = UrlDownloader()
    daily_zip_dir = tmp_path / 'dailyzipfiles'
    qrtr_zip_dir = tmp_path / 'qrtzipfiles'
    os.makedirs(daily_zip_dir)
    os.makedirs(qrtr_zip_dir)
    rapid_api_key = os.environ.get('RAPID_API_KEY')

    rapidurlbuilder = RapidUrlBuilder(rapid_plan='basic', rapid_api_key=rapid_api_key)
    yield RapidZipDownloader(rapidurlbuilder=rapidurlbuilder,
                             qrtr_zip_dir=str(qrtr_zip_dir),
                             daily_zip_dir=str(daily_zip_dir),
                             urldownloader=url_downloader,
                             execute_serial=True)


def test_get_content(rapidzipdownloader):
    # check if the call to the api is working
    content = rapidzipdownloader._get_content()
    parsed_json = json.loads(content)

    assert parsed_json['daily']


def test_get_zip_dir(rapidzipdownloader):
    assert rapidzipdownloader.zip_dir.endswith("dailyzipfiles")


def test_get_available_zips(rapidzipdownloader):
    content: str = """{
          "daily": [
                  {  
                    "date": "2022-01-03",
                    "file": "20220103.zip",
                    "subscription": "basic"
                  },
                  {  
                    "date": "2022-01-04",
                    "file": "20220104.zip",
                    "subscription": "basic"
                  }, 
                  {  
                    "date": "2022-01-05",
                    "file": "20220105.zip",
                    "subscription": "basic"
                  }, 
                  {  
                    "date": "2022-01-06",
                    "file": "20220106.zip",
                    "subscription": "premium"
                  },
                  {  
                    "date": "2022-01-07",
                    "file": "20220107.zip",
                    "subscription": "premium"
                  }
                  ]}"""
    get_content_mock = MagicMock()
    rapidzipdownloader._get_content = get_content_mock

    get_content_mock.return_value = content

    base_entries = rapidzipdownloader._get_available_zips()
    assert len(base_entries) == 3

    rapidzipdownloader.rapidurlbuilder.rapid_plan = 'premium'

    base_entries = rapidzipdownloader._get_available_zips()
    assert len(base_entries) == 5


def test_get_latest_quarter_file_name(rapidzipdownloader):
    with patch('secfsdstools.c_download.rapiddownloading.get_filenames_in_directory',
               return_value=['2022q3.zip', '2022q4.zip', '2022q2.zip']):
        assert rapidzipdownloader._get_latest_quarter_file_name() == '2022q4.zip'


def test__calculate_cut_off_for_qrtr_file(rapidzipdownloader):
    assert rapidzipdownloader._calculate_cut_off_for_qrtr_file('2022q1.zip') == '20220400'
    assert rapidzipdownloader._calculate_cut_off_for_qrtr_file('2022q2.zip') == '20220700'
    assert rapidzipdownloader._calculate_cut_off_for_qrtr_file('2022q3.zip') == '20221000'
    assert rapidzipdownloader._calculate_cut_off_for_qrtr_file('2022q4.zip') == '20230100'


def test_calculate_missing_zips(rapidzipdownloader):
    latest_quarter_file_mock = MagicMock()
    rapidzipdownloader._get_latest_quarter_file_name = latest_quarter_file_mock

    downloaded_zip_mock = MagicMock()
    rapidzipdownloader._get_downloaded_zips = downloaded_zip_mock

    available_zips_mock = MagicMock()
    rapidzipdownloader._get_available_zips = available_zips_mock

    latest_quarter_file_mock.return_value = '2022q4.zip'
    available_zips_mock.return_value = ['20221230.zip', '20221231.zip', '20230101.zip', '20230102.zip']

    downloaded_zip_mock.return_value = []
    result = rapidzipdownloader._calculate_missing_zips()

    assert len(result) == 2
    for entry in result:
        assert entry[0] in ['20230101.zip', '20230102.zip']
        assert entry[1] in ['https://daily-sec-financial-statement-dataset.p.rapidapi.com/basic/day/2023-01-01/',
                            'https://daily-sec-financial-statement-dataset.p.rapidapi.com/basic/day/2023-01-02/']
