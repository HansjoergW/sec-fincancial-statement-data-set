"""
Contains BaseDownloader class.
"""

import logging
import os
from abc import ABC, abstractmethod
from typing import Tuple, List, Dict

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_filenames_in_directory
from secfsdstools.a_utils.parallelexecution import ParallelExecutor

LOGGER = logging.getLogger(__name__)


class BaseDownloader(ABC):
    """
    Base class for Downloaders. Implements basic methods to download files
    from an url and store it.
    """

    def __init__(self, zip_dir: str, urldownloader: UrlDownloader, execute_serial: bool = False):
        self.urldownloader = urldownloader
        self.execute_serial = execute_serial

        self.result = None

        self.zip_dir = zip_dir

        if not os.path.isdir(self.zip_dir):
            LOGGER.info("creating download folder: %s", self.zip_dir)
            os.makedirs(self.zip_dir)

    def _get_headers(self) -> Dict[str, str]:
        return {}

    def _download_zip(self, file: str, url: str) -> str:
        file_path = os.path.join(self.zip_dir, file)
        try:
            self.urldownloader.binary_download_url_to_file(url, file_path,
                                                           headers=self._get_headers())
            return 'success'
        except Exception as ex:  # pylint: disable=W0703
            # we want to catch everything here.
            return f'failed: {ex}'

    def _download_file(self, data: Tuple[str, str]) -> str:
        file: str = data[0]
        url: str = data[1]

        LOGGER.info('    start to download %s ', file)
        result = self._download_zip(url=url, file=file)
        return result

    def _get_downloaded_zips(self) -> List[str]:
        return get_filenames_in_directory(os.path.join(self.zip_dir, '*.zip'))

    @abstractmethod
    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        pass

    def download(self):
        """
        downloads the missing quarterly zip files from the sec.
        """

        executor = ParallelExecutor[Tuple[str, str], str, type(None)](
            max_calls_per_sec=8,
            chunksize=20,
            execute_serial=True
            # execute_serial=self.execute_serial
        )
        executor.set_get_entries_function(self._calculate_missing_zips)
        executor.set_process_element_function(self._download_file)
        executor.set_post_process_chunk_function(lambda x: x)

        self.result = executor.execute()
