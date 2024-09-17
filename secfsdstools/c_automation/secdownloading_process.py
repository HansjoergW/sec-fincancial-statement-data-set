"""
Downloading zip files of the financial statement data sets from the sec.
"""
import logging
import os
import re
from abc import abstractmethod
from typing import List, Tuple

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_filenames_in_directory, get_directories_in_directory
from secfsdstools.c_automation.task_framework import AbstractProcess, Task

LOGGER = logging.getLogger(__name__)


class DownloadTask:

    def __init__(self,
                 zip_dir: str,
                 file_name: str,
                 url: str,
                 urldownloader: UrlDownloader
                 ):
        self.zip_dir = zip_dir
        self.file_name = file_name
        self.url = url
        self.urldownloader = urldownloader

        def prepare(self):
            """ """
            pass

        def execute(self):
            """ """
            file_path = os.path.join(self.zip_dir, self.file_name)
            try:
                self.urldownloader.binary_download_url_to_file(url, file_path,
                                                               headers=self._get_headers())
                return 'success'
            except Exception as ex:  # pylint: disable=W0703
                # we want to catch everything here.
                return f'failed: {ex}'
        def commit(self):
            """ """
            pass

        def post_commit(self):
            """ """

        def exception(self, exception):
            """ """

class BaseDownloadingProcess(AbstractProcess):

    def __init__(self,
                 zip_dir: str,
                 parquet_dir: str,
                 urldownloader: UrlDownloader,
                 execute_serial: bool = False):
        self.zip_dir = zip_dir
        self.parquet_dir = parquet_dir
        self.urldownloader = urldownloader
        self.execute_serial = execute_serial

        if not os.path.isdir(self.zip_dir):
            LOGGER.info("creating download folder: %s", self.zip_dir)
            os.makedirs(self.zip_dir)

    def _get_downloaded_zips(self) -> List[str]:
        return get_filenames_in_directory(os.path.join(self.zip_dir, '*.zip'))

    def _get_transformed_parquet(self) -> List[str]:
        return get_directories_in_directory(self.parquet_dir)

    @abstractmethod
    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        pass

    def calculate_tasks(self) -> List[Task]:
        missing_zips: List[Tuple[str, str]] = self._calculate_missing_zips()

        tasks: List[Task] = []
        for name, href in missing_zips:
            pass

        return tasks


class SecDownloadingProcess(BaseDownloadingProcess):
    """
        Downloading the quarterly zip files of the financial statement data sets
    """
    FIN_STAT_DATASET_URL = 'https://www.sec.gov/dera/data/financial-statement-data-sets.html'

    table_re = re.compile('<TABLE.*?>.*</TABLE>', re.IGNORECASE + re.MULTILINE + re.DOTALL)
    href_re = re.compile("href=\".*?\"", re.IGNORECASE + re.MULTILINE + re.DOTALL)

    def __init__(self,
                 zip_dir: str,
                 parquet_root_dir: str,
                 urldownloader: UrlDownloader,
                 execute_serial: bool = False):
        super().__init__(zip_dir=zip_dir,
                         urldownloader=urldownloader,
                         parquet_dir=os.path.join(parquet_root_dir, 'quarter'),
                         execute_serial=execute_serial
                         )

    def _get_available_zips(self) -> List[Tuple[str, str]]:
        content = self.urldownloader.get_url_content(self.FIN_STAT_DATASET_URL)
        first_table = self.table_re.findall(content.text)[0]
        hrefs = self.href_re.findall(first_table)

        hrefs = [f'https://www.sec.gov{href[6:-1]}' for href in hrefs]
        return [(os.path.basename(href), href) for href in hrefs]

    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        downloaded_zip_files = self._get_downloaded_zips()
        transformed_parquet = self._get_transformed_parquet()
        available_zips_to_dld_dict = self._get_available_zips()

        # define which zip files don't have to be downloaded
        download_or_transformed_zips = set(downloaded_zip_files).union(set(transformed_parquet))

        return [(name, href) for name, href in available_zips_to_dld_dict if
                name not in download_or_transformed_zips]
