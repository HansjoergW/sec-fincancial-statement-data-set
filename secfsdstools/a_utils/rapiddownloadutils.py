"""
Utils to help with downloading from the rapid api website
"""

from typing import Dict


class RapidUrlBuilder:
    """
    Class which provides simple methods to get the rapid api urls
    """

    RAPID_HOST = "daily-sec-financial-statement-dataset.p.rapidapi.com"
    RAPID_URL = "https://" + RAPID_HOST

    RAPID_CONTENT_URL = RAPID_URL + "/content/"
    RAPID_HEARTBEAT_URL = RAPID_URL + "/heartbeat/"

    def __init__(self, rapid_api_key: str, rapid_plan: str):
        """
        Constructor.
        Args:
            rapid_api_key (str): the rapid_api_key that should be used to make the calls
            rapid_plan (str): the subscription plan for the Daily-Sec-Finaincial-Statement-Dataset
             on rapid.api. either basic or premium
        """
        self.rapid_api_key = rapid_api_key
        self.rapid_plan = rapid_plan

    def get_donwload_url(self, filename: str) -> str:
        """
        creates the download url for the provided daily zipfile name
        Args:
            filename (str): daily zipfile name in the format <YYYYMMDD.zip>

        Returns:
            str, the url

        """
        return RapidUrlBuilder.RAPID_URL + \
               f'/{self.rapid_plan}/day/{filename[:4]}-{filename[4:6]}-{filename[6:8]}/'

    def get_content_url(self) -> str:
        """
        Returns the url to download the json with the content.
        Returns:
            str: url to download the content json file
        """
        return RapidUrlBuilder.RAPID_CONTENT_URL

    def get_heartbeat_url(self) -> str:
        """
        gets the heartbeat url (to check if the rapid api is up and running)
        Returns:
            str: the url to get the heartbeat
        """
        return RapidUrlBuilder.RAPID_HEARTBEAT_URL

    def get_headers(self) -> Dict[str, str]:
        """
        creates a dict with the necessary http headers.

        Returns:
            Dict[str, str]: Dict with the necessary heaers
        """
        return {
            "X-RapidAPI-Key": f"{self.rapid_api_key}",
            "X-RapidAPI-Host": f"{RapidUrlBuilder.RAPID_HOST}"
        }
