import logging
from typing import List

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.c_download.basedownloading_process import DownloadTask
from secfsdstools.c_download.secdownloading_process import SecDownloadingProcess
from secfsdstools.c_automation.task_framework import Task


class FailingDownload(SecDownloadingProcess):
    def calculate_tasks(self) -> List[Task]:
        tasks = super().calculate_tasks()
        tasks.append(DownloadTask(
            zip_dir=self.zip_dir,
            urldownloader=self.urldownloader,
            file_name="notexisting.zip",
            url="https://www.seg.gov/myfile.zip"
        ))
        return tasks


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    process = FailingDownload(zip_dir="./tmp/zips",
                              parquet_root_dir="./tmp/parquets",
                              urldownloader=UrlDownloader(user_agent="me@miracle.com"),
                              execute_serial=False)

    process.process()
