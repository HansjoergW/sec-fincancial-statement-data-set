import json
import logging
import os
from typing import List, Tuple, Dict

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_filenames_in_directory
from secfsdstools.c_download.basedownloading import BaseDownloader

LOGGER = logging.getLogger(__name__)


class RapidZipDownloader(BaseDownloader):
    RAPID_HOST = "daily-sec-financial-statement-dataset.p.rapidapi.com"
    RAPID_URL = "https://" + RAPID_HOST

    RAPID_CONTENT_URL = RAPID_URL + "/content/"

    def __init__(self, rapid_api_key: str, rapid_plan: str, zip_dir: str, urldownloader: UrlDownloader,
                 execute_serial: bool = False):
        super().__init__(zip_dir=zip_dir, urldownloader=urldownloader, execute_serial=execute_serial)
        self.rapid_api_key = rapid_api_key
        self.rapid_plan = rapid_plan

        self.qrtr_zip_dir = zip_dir

        # overwrite zipdir
        self.zip_dir = os.path.join(self.zip_dir, "daily")

        if not os.path.isdir(self.zip_dir):
            LOGGER.info("creating download folder: %s", self.zip_dir)
            os.makedirs(self.zip_dir)

    def _get_headers(self) -> Dict[str, str]:
        return {
            "X-RapidAPI-Key": f"{self.rapid_api_key}",
            "X-RapidAPI-Host": f"{RapidZipDownloader.RAPID_HOST}"
        }

    def _get_content(self) -> str:
        response = self.urldownloader.get_url_content(RapidZipDownloader.RAPID_CONTENT_URL, headers=self._get_headers())
        return response.text

    def _get_latest_quarter_file_name(self):
        files = get_filenames_in_directory(os.path.join(self.qrtr_zip_dir, '*.zip'))
        files.sort(reverse=True)
        return files[0]

    def _calculate_cut_off_for_qrtr_file(self, filename: str) -> str:
        last_quarter_file_year = int(filename[:4])
        last_quarter_file_quarter = int(filename[5:6])

        cutoff: str = ''
        if last_quarter_file_quarter < 4:
            cutoff = str(last_quarter_file_year) + str(((last_quarter_file_quarter * 3) + 1)).zfill(2) + '00'
        else:
            cutoff = str(last_quarter_file_year + 1) + '0100'
        return cutoff

    def _calculate_donwload_url(self, filename: str) -> str:
        return RapidZipDownloader.RAPID_URL + f'/{self.rapid_plan}/{filename[:4]}-{filename[4:6]}-{filename[6:8]}/'

    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        # only download the daily zips for dates for which there is no quarter zip file yet
        # so first get that latest downloaded zip -> this is always done first
        latest_quarter_file = self._get_latest_quarter_file_name()

        # then calculate the cut_off string
        # e.g., if the lates zip is 2022q4.zip, then the cutoff string looks like: '20230100'
        cutoff_str = self._calculate_cut_off_for_qrtr_file(latest_quarter_file)

        dld_zip_files = self._get_downloaded_zips()
        available_zips = self._get_available_zips()

        missing = list(set(available_zips) - set(dld_zip_files))

        # only consider the filenames with names (without extension) are bigger than the cutoff string
        missing_after_cut_off = [entry for entry in missing if entry[:8] > cutoff_str]

        missing_tuple = [(filename, self._calculate_donwload_url(filename)) for filename in missing_after_cut_off]
        return missing_tuple

    def _get_available_zips(self) -> List[str]:
        content = self._get_content()
        parsed_content = json.loads(content)
        daily_entries = parsed_content['daily']

        available_files = [entry['file'] for entry in daily_entries if
                           ((entry['subscription'] == 'basic') | (entry['subscription'] == self.rapid_plan))]

        return available_files
