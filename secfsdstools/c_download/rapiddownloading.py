"""
Logic to download the zipfiles from the rapid api.
"""

import json
import logging
import os
from typing import List, Tuple, Dict

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_filenames_in_directory
from secfsdstools.a_utils.rapiddownloadutils import RapidUrlBuilder
from secfsdstools.c_download.basedownloading import BaseDownloader

LOGGER = logging.getLogger(__name__)


class RapidZipDownloader(BaseDownloader):
    """
    Class which coordinates downloading form the rapidapi api
    https://rapidapi.com/hansjoerg.wingeier/api/daily-sec-financial-statement-dataset
    """

    def __init__(self, rapidurlbuilder: RapidUrlBuilder,
                 daily_zip_dir: str,
                 qrtr_zip_dir: str,
                 parquet_root_dir: str,
                 urldownloader: UrlDownloader,
                 execute_serial: bool = False):
        super().__init__(zip_dir=daily_zip_dir,
                         urldownloader=urldownloader,
                         execute_serial=execute_serial,
                         parquet_dir_typed=os.path.join(parquet_root_dir, 'quarter'))
        self.rapidurlbuilder = rapidurlbuilder

        self.qrtr_zip_dir = qrtr_zip_dir

        if not os.path.isdir(self.zip_dir):
            LOGGER.info("creating download folder: %s", self.zip_dir)
            os.makedirs(self.zip_dir)

    def _get_headers(self) -> Dict[str, str]:
        return self.rapidurlbuilder.get_headers()

    def _get_content(self) -> str:
        response = self.urldownloader.get_url_content(self.rapidurlbuilder.get_content_url(),
                                                      headers=self._get_headers())
        return response.text

    def _get_latest_quarter_file_name(self):
        files = get_filenames_in_directory(os.path.join(self.qrtr_zip_dir, '*.zip'))
        files.sort(reverse=True)
        return files[0]

    def _calculate_cut_off_for_qrtr_file(self, filename: str) -> str:
        """
        We only want to download daily files that are newer than the latest available
        quarter file on the SEC side.

        The idea is simple, we create a date-string of the quarter that starts after the quarter
        defined in the filename, but we set the day to 00.

        So if the filename is 2022q1
        -> "2022" + "04" + "00"
        if it is 2022q4
        -> "2022" + "0100"

        Args:
            filename: filname of the quarter file

        Returns:

        """
        last_quarter_file_year = int(filename[:4])
        last_quarter_file_quarter = int(filename[5:6])

        cutoff: str = ''
        if last_quarter_file_quarter < 4:
            cutoff = str(last_quarter_file_year) \
                     + str(((last_quarter_file_quarter * 3) + 1)).zfill(2) \
                     + '00'
        else:
            cutoff = str(last_quarter_file_year + 1) + '0100'
        return cutoff

    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        # only download the daily zips for dates for which there is no quarter zip file yet
        # so first get that latest downloaded zip -> this is always done first
        latest_quarter_file = self._get_latest_quarter_file_name()

        # then calculate the cut_off string
        # e.g., if the lates zip is 2022q4.zip, then the cutoff string looks like: '20230100'
        cutoff_str = self._calculate_cut_off_for_qrtr_file(latest_quarter_file)

        downloaded_zip_files = self._get_downloaded_zips()
        transformed_parquet = self._get_transformed_parquet()
        available_zips_to_dld = self._get_available_zips()

        # define which zip files don't have to be downloaded
        download_or_transformed_zips = set(downloaded_zip_files).union(set(transformed_parquet))

        missing = list(set(available_zips_to_dld) - set(download_or_transformed_zips))

        # only consider the filenames with names (without extension)
        # that are bigger than the cutoff string
        missing_after_cut_off = [entry for entry in missing if entry[:8] > cutoff_str]

        return [(filename, self.rapidurlbuilder.get_donwload_url(filename)) for filename in
                missing_after_cut_off]

    def _get_available_zips(self) -> List[str]:
        content = self._get_content()
        parsed_content = json.loads(content)
        daily_entries = parsed_content['daily']

        return [entry['file'] for entry in daily_entries if
                ((entry['subscription'] == 'basic') | (
                        entry['subscription'] == self.rapidurlbuilder.rapid_plan))]
