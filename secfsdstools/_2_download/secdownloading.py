"""
Downloading zip files of the financial statement data sets from the sec.
"""
import glob
import logging
import os
import re
from typing import List, Tuple

from secfsdstools._0_utils.downloadutils import UrlDownloader
from secfsdstools._0_utils.parallelexecution import ParallelExecutor

LOGGER = logging.getLogger(__name__)


class SecZipDownloader:
    """
        Downloading the quarterly zip files of the financial statement data sets
    """
    FIN_STAT_DATASET_URL = 'https://www.sec.gov/dera/data/financial-statement-data-sets.html'

    table_re = re.compile('<TABLE.*?>.*</TABLE>', re.IGNORECASE + re.MULTILINE + re.DOTALL)
    href_re = re.compile("href=\".*?\"", re.IGNORECASE + re.MULTILINE + re.DOTALL)

    def __init__(self, zip_dir: str, urldownloader: UrlDownloader, execute_serial: bool = False):
        self.urldownloader = urldownloader
        self.execute_serial = execute_serial

        self.result = None

        if zip_dir[-1] != '/':
            zip_dir = zip_dir + '/'
        self.zip_dir = zip_dir

        if not os.path.isdir(self.zip_dir):
            LOGGER.info("creating download folder: %s", self.zip_dir)
            os.makedirs(self.zip_dir)

    def _get_downloaded_list(self) -> List[str]:
        zip_list: List[str] = glob.glob(self.zip_dir + '*.zip')
        return [os.path.basename(x) for x in zip_list]

    def _get_available_zips(self) -> List[Tuple[str, str]]:
        content = self.urldownloader.get_url_content(self.FIN_STAT_DATASET_URL)
        first_table = self.table_re.findall(content.text)[0]
        hrefs = self.href_re.findall(first_table)

        hrefs = ['https://www.sec.gov' + href[6:-1] for href in hrefs]
        return [(os.path.basename(href), href) for href in hrefs]

    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        dld_zip_files = self._get_downloaded_list()
        zips_to_dld_dict = self._get_available_zips()

        return [(name, href) for name, href in zips_to_dld_dict if name not in dld_zip_files]

    def _download_zip(self, url: str, file: str) -> str:
        file_path = self.zip_dir + file
        try:
            self.urldownloader.binary_download_url_to_file(url, file_path)
            return 'success'
        except Exception as ex:  # pylint: disable=W0703
            # we want to catch everything here.
            return f'failed: {ex}'

    def _download_file(self, data: Tuple[str, str]) -> str:
        file: str = data[0]
        url: str = data[1]

        # todo: logging does not work like that when using multiprocessing
        #  see https://superfastpython.com/multiprocessing-logging-in-python/
        # LOGGER.info('    start to download %s ', file)
        result = self._download_zip(url, file)
        # LOGGER.info('    %s - %s', file, result)
        return result

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
        executor.pool.close()
