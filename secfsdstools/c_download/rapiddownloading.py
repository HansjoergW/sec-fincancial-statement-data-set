import logging
import os
from typing import List, Tuple, Dict

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.c_download.basedownloading import BaseDownloader

LOGGER = logging.getLogger(__name__)


class RapidZipDownloader(BaseDownloader):
    RAPID_HOST = "daily-sec-financial-statement-dataset.p.rapidapi.com"
    RAPID_URL = "https://" + RAPID_HOST

    RAPID_CONTENT_URL = RAPID_URL + "/content/"

    def __init__(self, rapid_api_key: str, zip_dir: str, urldownloader: UrlDownloader, execute_serial: bool = False):
        super().__init__(zip_dir=zip_dir, urldownloader=urldownloader, execute_serial=execute_serial)
        self.rapid_api_key = rapid_api_key

        # overwrite zipdir
        # todo: noch nicht ideal mit storage verzeichnis...
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

    def _calculate_missing_zips(self) -> List[Tuple[str, str]]:
        return []

    def _get_available_zips(self) -> List[Tuple[str, str]]:
        return []
        # content = self.urldownloader.get_url_content(self.FIN_STAT_DATASET_URL)
        # first_table = self.table_re.findall(content.text)[0]
        # hrefs = self.href_re.findall(first_table)
        #
        # hrefs = ['https://www.sec.gov' + href[6:-1] for href in hrefs]
        # return [(os.path.basename(href), href) for href in hrefs]
