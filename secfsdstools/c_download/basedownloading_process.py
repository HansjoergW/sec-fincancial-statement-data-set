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
from secfsdstools.c_automation.task_framework import AbstractProcess

LOGGER = logging.getLogger(__name__)


class DownloadTask:
    """
    This Tasks takes care of downloading a file from a certain url.
    """

    def __init__(self,
                 target_path: Path,
                 url: str,
                 urldownloader: UrlDownloader,
                 headers=None
                 ):
        """
        Constructor.
        Args:
            target_path: target path to which the resource has to be stored
            url: url to download
            urldownloader: instance of the UrlDownloader
            headers: special headers to be added to the http call as dict.Default is an empty dict
        """
        if headers is None:
            headers = {}
        self.url = url
        self.urldownloader = urldownloader
        self.headers = headers

        self.target_path = target_path

    def prepare(self):
        """
        create the target directory (incl. parents) if necessary
        """
        self.target_path.parent.mkdir(parents=True, exist_ok=True)

    def execute(self):
        """ download the url using the urldownloader instance. """
        logger = logging.getLogger()
        logger.info("download %s", self.url)
        self.urldownloader.binary_download_url_to_file(self.url,
                                                       str(self.target_path),
                                                       headers=self.headers)

    def commit(self) -> str:
        """ no real commit logic necessary, just return 'success' """
        return "success"

    def exception(self, exception) -> str:
        """ no special failure handling necessary"""
        return f"failed {exception}"

    def __str__(self) -> str:
        return f"DownloadTask(target_path: {self.target_path})"


class BaseDownloadingProcess(AbstractProcess):
    """
    Base implementation for downloading zip files.
    """

    def __init__(self,
                 zip_dir: str,
                 parquet_dir: str,
                 urldownloader: UrlDownloader,
                 execute_serial: bool = False):
        """
        Constructor.
        Args:
            zip_dir: target folder to store downloaded files to.
            parquet_dir: directory where the transformed content of zip files are
            urldownloader: UrlDownloader instance
            execute_serial: whether to execute it in parallel or serial
        """
        super().__init__(execute_serial=execute_serial,
                         chunksize=3)

        self.zip_dir = zip_dir
        self.parquet_dir = parquet_dir
        self.urldownloader = urldownloader
        self.execute_serial = execute_serial

        if not os.path.isdir(self.zip_dir):
            LOGGER.info("creating download folder: %s", self.zip_dir)
            os.makedirs(self.zip_dir)

    def get_headers(self) -> Dict[str, str]:
        """
        return the needed headers. No additional header needed in this case.
        """
        return {}

    def _get_downloaded_zips(self) -> List[str]:
        """
        return the list of zipfiles that are already downloaded.
        """
        return get_filenames_in_directory(os.path.join(self.zip_dir, '*.zip'))

    def _get_transformed_parquet(self) -> List[str]:
        """ return the list of zipfiles that were already transformed to parquet."""
        return get_directories_in_directory(self.parquet_dir)

    @abstractmethod
    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        """ calculate the zip files, which still need to be downloaded"""

    def calculate_tasks(self) -> List[DownloadTask]:
        """
        create the download tasks: a task for every missing zip file
        Returns:
            List[DownloadTask]: list of download tasks

        """
        missing_zips: List[Tuple[str, str]] = self._calculate_missing_zips()

        return [DownloadTask(target_path=Path(self.zip_dir) / name,
                             url=href,
                             urldownloader=self.urldownloader,
                             headers=self.get_headers())
                for name, href in missing_zips]
