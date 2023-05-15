"""
Downloading zip files of the financial statement data sets from the sec.
"""
import logging
import os
import re
from typing import List, Tuple

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.c_download.basedownloading import BaseDownloader

LOGGER = logging.getLogger(__name__)


class SecZipDownloader(BaseDownloader):
    """
        Downloading the quarterly zip files of the financial statement data sets
    """
    FIN_STAT_DATASET_URL = 'https://www.sec.gov/dera/data/financial-statement-data-sets.html'

    table_re = re.compile('<TABLE.*?>.*</TABLE>', re.IGNORECASE + re.MULTILINE + re.DOTALL)
    href_re = re.compile("href=\".*?\"", re.IGNORECASE + re.MULTILINE + re.DOTALL)

    def __init__(self, zip_dir: str, parquet_root_dir: str,
                 urldownloader: UrlDownloader, execute_serial: bool = False):
        super().__init__(zip_dir=zip_dir, urldownloader=urldownloader,
                         parquet_dir_typed=os.path.join(parquet_root_dir, 'quarter'),
                         execute_serial=execute_serial)

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
