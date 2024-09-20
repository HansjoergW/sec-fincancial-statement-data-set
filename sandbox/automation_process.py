from secfsdstools.a_utils.downloadutils import UrlDownloader

from secfsdstools.c_automation.secdownloading_process import SecDownloadingProcess

if __name__ == '__main__':
    process = SecDownloadingProcess(zip_dir="./tmp/zips",
                                    parquet_root_dir="./tmp/parquets",
                                    urldownloader=UrlDownloader(user_agent="me@miracle.com"),
                                    execute_serial=True)

    process.process()