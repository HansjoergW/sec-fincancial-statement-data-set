"""
Downloading zip files of the financial statement data sets from the sec.
"""
import logging
import os
import re
from typing import List, Tuple

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.c_download.basedownloading_process import BaseDownloadingProcess

LOGGER = logging.getLogger(__name__)


class SecDownloadingProcess(BaseDownloadingProcess):
    """
        Downloading the quarterly zip files of the financial statement data sets
    """
    FIN_STAT_DATASET_URL = 'https://www.sec.gov/dera/data/financial-statement-data-sets.html'
    # FIN_STAT_DATASET_ARCHIVE_URL = \
    #     'https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets-archive'

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

        # # reading data from the archived page - until 2024q3.zip
        # LOGGER.info("reading table in archive: %s", self.FIN_STAT_DATASET_ARCHIVE_URL)
        # archive_content = self.urldownloader.get_url_content(self.FIN_STAT_DATASET_ARCHIVE_URL)
        # archive_tables = self.table_re.findall(archive_content.text)
        #
        # archive_hrefs: List[str] = []
        #
        # if len(archive_tables) == 0:
        #     LOGGER.warning("No archive table found at: %s", self.FIN_STAT_DATASET_ARCHIVE_URL)
        # else:
        #     archive_first_table = archive_tables[0]
        #     archive_hrefs = self.href_re.findall(archive_first_table)
        #     archive_hrefs = [f'https://www.sec.gov{href[6:-1]}' for href in archive_hrefs]

        # reading data from the main url - starting with 2024q4.zip
        LOGGER.info("reading table in main page: %s", self.FIN_STAT_DATASET_URL)
        main_content = self.urldownloader.get_url_content(self.FIN_STAT_DATASET_URL)
        main_tables = self.table_re.findall(main_content.text)

        main_hrefs: List[str] = []

        if len(main_tables) == 0:
            LOGGER.warning("No table found at: %s", self.FIN_STAT_DATASET_URL)
        else:
            main_first_table = main_tables[0]
            main_hrefs = self.href_re.findall(main_first_table)
            main_hrefs = [f'https://www.sec.gov{href[6:-1]}' for href in main_hrefs]

        # hrefs = archive_hrefs + main_hrefs
        # return_value: List[Tuple[str, str]] = [(os.path.basename(href), href) for href in hrefs]
        #
        # return_value = [(n.replace("-archive", ""), p) for n, p in return_value]
        # return return_value

        return_value: List[Tuple[str, str]] = \
            [(os.path.basename(href), href) for href in main_hrefs]
        return return_value

    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        downloaded_zip_files = self._get_downloaded_zips()
        transformed_parquet = self._get_transformed_parquet()
        available_zips_to_dld_dict = self._get_available_zips()

        # define which zip files don't have to be downloaded
        download_or_transformed_zips = set(downloaded_zip_files).union(set(transformed_parquet))

        return [(name, href) for name, href in available_zips_to_dld_dict if
                name not in download_or_transformed_zips]
