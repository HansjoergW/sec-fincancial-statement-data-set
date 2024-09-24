"""
Base classes for Downloading zip files of the financial statement data sets.
"""
import logging
import os
from abc import abstractmethod
from pathlib import Path
from typing import List, Tuple, Dict

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_filenames_in_directory, get_directories_in_directory
from secfsdstools.c_automation.task_framework import AbstractProcess, Task

LOGGER = logging.getLogger(__name__)


class DownloadTask:

    def __init__(self,
                 zip_dir: str,
                 file_name: str,
                 url: str,
                 urldownloader: UrlDownloader,
                 headers: Dict[str, str] = {}
                 ):
        self.zip_dir = zip_dir
        self.file_name = file_name
        self.url = url
        self.urldownloader = urldownloader
        self.headers = headers

        self.file_path = Path(self.zip_dir) / self.file_name

    def prepare(self):
        """ """
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def execute(self):
        """ """
        logger = logging.getLogger()
        logger.info("download %s", self.url)
        self.urldownloader.binary_download_url_to_file(self.url,
                                                       str(self.file_path),
                                                       headers=self.headers)

    def commit(self) -> str:
        """ """
        return "success"

    def post_commit(self):
        """ """
        pass

    def exception(self, exception) -> str:
        """ """
        return f"failed {exception}"

    def __str__(self) -> str:
        return f"DownloadTask(file_name: {self.file_name})"


class BaseDownloadingProcess(AbstractProcess):

    def __init__(self,
                 zip_dir: str,
                 parquet_dir: str,
                 urldownloader: UrlDownloader,
                 execute_serial: bool = False):
        super().__init__(execute_serial=execute_serial)

        self.zip_dir = zip_dir
        self.parquet_dir = parquet_dir
        self.urldownloader = urldownloader
        self.execute_serial = execute_serial

        if not os.path.isdir(self.zip_dir):
            LOGGER.info("creating download folder: %s", self.zip_dir)
            os.makedirs(self.zip_dir)

    def get_headers(self) -> Dict[str, str]:
        return {}

    def _get_downloaded_zips(self) -> List[str]:
        return get_filenames_in_directory(os.path.join(self.zip_dir, '*.zip'))

    def _get_transformed_parquet(self) -> List[str]:
        return get_directories_in_directory(self.parquet_dir)

    @abstractmethod
    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        pass

    def calculate_tasks(self) -> List[Task]:
        missing_zips: List[Tuple[str, str]] = self._calculate_missing_zips()

        return [DownloadTask(zip_dir=self.zip_dir,
                             file_name=name,
                             url=href,
                             urldownloader=self.urldownloader,
                             headers=self.get_headers())
                for name, href in missing_zips]
