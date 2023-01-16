"""
Download utils to download data from the SEC website.
"""

import logging
from time import sleep

import requests

from secfsdstools.a_utils.fileutils import write_content_to_zip

LOGGER = logging.getLogger(__name__)


class UrlDownloader:
    """
    Main downloader class
    """

    def __init__(self, user_agent: str = "<not set>"):
        """
        Args:
            user_agent (str): according to https://www.sec.gov/os/accessing-edgar-data in the form
        User-Agent: Sample Company Name AdminContact@<sample company domain>.com
        """

        self.user_agent = user_agent

    def download_url_to_file(self, file_url: str, target_file: str,
                             expected_size: int = None,
                             max_tries: int = 6,
                             sleep_time: int = 1):
        """
            downloads the content auf an url and stores it into the target-file.
            retries a download several times, if it fails

        Args:
            file_url (str): url that referencese the file to be downloaded
            target_file (str): the file to store the content into
               (it will be written into a zipfile)
            expected_size (str, optional, None): the expected size of
              the data that is downloaded.
            logs a warning if the size doesn't match
            max_tries (int, optional, 6): maximum retries, default is 6
            sleep_time (int, optional, 1): wait time between retries,
              default is one second

        Returns:
            str: the written file name
        """
        response = self.get_url_content(file_url, max_tries, sleep_time)
        content = response.text

        if expected_size is not None:
            if len(content) != expected_size:
                LOGGER.info('warning expected size %d - real size %d', expected_size, len(content))

        return write_content_to_zip(content, target_file)

    def binary_download_url_to_file(self, file_url: str,
                                    target_file: str,
                                    max_tries: int = 6,
                                    sleep_time: int = 1):
        """
            downloads the binary of an url and stores it into the target-file.
            retries a download several times, if it fails

        Args:
            file_url (str): url that referencese the file to be downloaded
            target_file (str): the file to store the content into
              (it will be written into a zipfile)
            max_tries (int, optional, 6): maximum retries, default is 6
            sleep_time (int, optional, 1): wait time between retries, default is one second
        """
        response = self.get_url_content(file_url, max_tries, sleep_time)

        with open(target_file, "wb") as target_fp:
            target_fp.write(response.content)

    def get_url_content(self, file_url: str, max_tries: int = 6, sleep_time: int = 1) \
            -> requests.models.Response:
        """
            downloads the content auf an url and returns it as a string.
            retries a download several times, if it fails.
            Uses the defined user-agent as header information

        Args:
            file_url (str): url that referencese the file to be downloaded
            max_tries (int, optional, 6): maximum number of tries to get the data
            sleep_time (int, optional, 1): wait time between retries, default is one second

        Returns:
             requests.models.Response
        """
        response = None
        current_try = 0
        while current_try < max_tries:
            current_try += 1
            try:
                response = requests.get(file_url, timeout=10,
                                        headers={'User-Agent': self.user_agent}, stream=True)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as err:
                if current_try >= max_tries:
                    LOGGER.info('RequestException: failed to download %s2', file_url)
                    raise err
                sleep(sleep_time)

        return response
