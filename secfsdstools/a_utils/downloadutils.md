Module secfsdstools.a_utils.downloadutils
=========================================
Download utils to download data from the SEC website.

Classes
-------

`UrlDownloader(user_agent: str = '<not set>')`
:   Main downloader class
    
    :param user_agent: according to https://www.sec.gov/os/accessing-edgar-data in the form
    User-Agent: Sample Company Name AdminContact@<sample company domain>.com

    ### Methods

    `binary_download_url_to_file(self, file_url: str, target_file: str, max_tries: int = 6, sleep_time: int = 1)`
    :   downloads the binary of an url and stores it into the target-file.
            retries a download several times, if it fails
        
        :param file_url: url that referencese the file to be downloaded
        :param target_file: the file to store the content into (it will be written into a zipfile)
        :param max_tries: (optional) maximum retries, default is 6
        :param sleep_time: (optional) wait time between retries, default is one second
        :return the written file

    `download_url_to_file(self, file_url: str, target_file: str, expected_size: int = None, max_tries: int = 6, sleep_time: int = 1)`
    :   downloads the content auf an url and stores it into the target-file.
            retries a download several times, if it fails
        
        :param file_url: url that referencese the file to be downloaded
        :param target_file: the file to store the content into (it will be written into a zipfile)
        :param expected_size: (optional) the expected size of the data that is downloaded.
                logs a warning if the size doesn't match
        :param max_tries: (optional) maximum retries, default is 6
        :param sleep_time: (optional) wait time between retries, default is one second
        :return the written file

    `get_url_content(self, file_url: str, max_tries: int = 6, sleep_time: int = 1) ‑> requests.models.Response`
    :   downloads the content auf an url and returns it as a string.
            retries a download several times, if it fails.
            Uses the defined user-agent as header information
        
        :param file_url: url that referencese the file to be downloaded
        :param expected_size: (optional) the expected size of the data that is downloaded.
            logs a warning if the size doesn't match
        :param sleep_time: (optional) wait time between retries, default is one second
        :return