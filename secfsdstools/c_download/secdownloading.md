Module secfsdstools.c_download.secdownloading
=============================================
Downloading zip files of the financial statement data sets from the sec.

Classes
-------

`SecZipDownloader(zip_dir: str, urldownloader: secfsdstools.a_utils.downloadutils.UrlDownloader, execute_serial: bool = False)`
:   Downloading the quarterly zip files of the financial statement data sets

    ### Class variables

    `FIN_STAT_DATASET_URL`
    :

    `href_re`
    :

    `table_re`
    :

    ### Methods

    `download(self)`
    :   downloads the missing quarterly zip files from the sec.