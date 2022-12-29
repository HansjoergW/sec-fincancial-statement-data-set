import logging
import os
import re
import shutil
import urllib.request
from typing import List, Dict
import glob

from secfsdstools.utils.downloadutils import UrlDownloader

LOGGER = logging.getLogger(__name__)


class SecZipDownloader:
    FIN_STAT_DATASET_URL = 'https://www.sec.gov/dera/data/financial-statement-data-sets.html'

    table_re = re.compile("<TABLE.*?>.*</TABLE>", re.IGNORECASE + re.MULTILINE + re.DOTALL)
    href_re = re.compile("href=\".*?\"", re.IGNORECASE + re.MULTILINE + re.DOTALL)

    def __init__(self, zip_dir: str, urldownloader: UrlDownloader):
        if zip_dir[-1] != '/':
            zip_dir = zip_dir + '/'
        self.zip_dir = zip_dir
        self.urldownloader = urldownloader

    def _get_downloaded_list(self) -> List[str]:
        zip_list: List[str] = glob.glob(self.zip_dir + '*.zip')
        return [os.path.basename(x) for x in zip_list]

    def _get_zips_to_download(self) -> Dict[str, str]:
        content = self.urldownloader.get_url_content(self.FIN_STAT_DATASET_URL)
        first_table = self.table_re.findall(content)[0]
        hrefs = self.href_re.findall(first_table)

        hrefs = ["https://www.sec.gov" + href[6:-1] for href in hrefs]
        return {os.path.basename(href): href for href in hrefs}

    def _download_zip(self, url: str, file_path: str) -> str:
        try:
            with urllib.request.urlopen(url, timeout=30) as urldata, open(file_path, 'wb') as out_file:
                shutil.copyfileobj(urldata, out_file)
                return "success"
        except Exception as ex:
            return "failed: {}".format(ex)

    def _download_missing(self, to_download_entries: Dict[str, str]):
        for file, url in to_download_entries.items():
            result = self._download_zip(url, self.zip_dir + file)
            LOGGER.info("    {} - {}".format(file, result))

    def download(self):
        dld_zip_files = self._get_downloaded_list()
        zips_to_dld_dict = self._get_zips_to_download()

        for k in dld_zip_files:
            zips_to_dld_dict.pop(k, None)
        LOGGER.info("downloading {} to {}".format(len(zips_to_dld_dict), self.zip_dir))
        self._download_missing(zips_to_dld_dict)
