"""
this module contains the update logic. This means downloading new zipfiles, transforming the data
into parquet format, and indexing the reports.
"""
import logging
import sys
import time
from pathlib import Path
from typing import List

import fastparquet

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.dbutils import DBStateAcessor
from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.a_utils.rapiddownloadutils import RapidUrlBuilder
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_automation.task_framework import AbstractProcess, execute_processes
from secfsdstools.c_download.rapiddownloading_process import RapidDownloadingProcess
from secfsdstools.c_download.secdownloading_process import SecDownloadingProcess
from secfsdstools.c_index.indexing_process import ReportParquetIndexerProcess
from secfsdstools.c_transform.toparquettransforming_process import ToParquetTransformerProcess

LOGGER = logging.getLogger(__name__)


class Updater:
    """Manages the update process: download zipfiles, transform to parquet, and index the reports"""
    LAST_UPDATE_CHECK_KEY: str = 'LAST_UPDATED'
    CHECK_EVERY_SECONDS: int = 24 * 60 * 60  # check every 24 hours

    @classmethod
    def get_instance(cls, config: Configuration):
        """
        Creates the Updater instance based on the provided Configuration object
        Args:
            config: Configuration object

        Returns:
            Updater: the instance

        """
        return Updater(
            config
        )

    def __init__(self,
                 config: Configuration):
        self.config = config

        self.db_state_accesor = DBStateAcessor(db_dir=config.db_dir)
        self.db_dir = config.db_dir
        self.dld_dir = config.download_dir
        self.daily_dld_dir = config.daily_download_dir
        self.parquet_dir = config.parquet_dir
        self.user_agent = config.user_agent_email
        self.rapid_api_plan = config.rapid_api_plan
        self.rapid_api_key = config.rapid_api_key
        self.keep_zip_files = config.keep_zip_files
        self.auto_update = config.auto_update
        self.no_parallel_processing = config.no_parallel_processing
        self.post_update_hook = config.post_update_hook
        self.post_update_processes = config.post_update_processes

    def _check_for_update(self) -> bool:
        """checks if a new update check should be conducted."""

        if self.db_state_accesor.table_exists(self.db_state_accesor.STATUS_TABLE_NAME) is False:
            return True

        last_check = self.db_state_accesor.get_key(Updater.LAST_UPDATE_CHECK_KEY)

        if last_check is None:
            return True

        return float(last_check) + Updater.CHECK_EVERY_SECONDS < time.time()

    def _build_process_list(self) -> List[AbstractProcess]:
        process_list: List[AbstractProcess] = []
        urldownloader = UrlDownloader(user_agent=self.user_agent)

        # download data from sec
        process_list.append(SecDownloadingProcess(zip_dir=self.dld_dir,
                                                  parquet_root_dir=self.parquet_dir,
                                                  urldownloader=urldownloader,
                                                  execute_serial=self.no_parallel_processing))
        # transform sec zip files
        process_list.append(ToParquetTransformerProcess(zip_dir=self.dld_dir,
                                                        parquet_dir=self.parquet_dir,
                                                        keep_zip_files=self.keep_zip_files,
                                                        file_type='quarter',
                                                        execute_serial=self.no_parallel_processing
                                                        ))

        # index sec zip files
        process_list.append(ReportParquetIndexerProcess(db_dir=self.db_dir,
                                                        parquet_dir=self.parquet_dir,
                                                        file_type='quarter',
                                                        execute_serial=self.no_parallel_processing
                                                        ))

        if (self.rapid_api_key is not None) & (self.rapid_api_key != ''):
            # download daily zip files
            rapidurlbuilder = RapidUrlBuilder(rapid_plan=self.rapid_api_plan,
                                              rapid_api_key=self.rapid_api_key)
            process_list.append(RapidDownloadingProcess(
                rapidurlbuilder=rapidurlbuilder,
                daily_zip_dir=self.daily_dld_dir,
                qrtr_zip_dir=self.dld_dir,
                urldownloader=urldownloader,
                parquet_root_dir=self.parquet_dir,
                execute_serial=self.no_parallel_processing))

            # transform daily zip files
            process_list.append(ToParquetTransformerProcess(
                zip_dir=self.daily_dld_dir,
                parquet_dir=self.parquet_dir,
                keep_zip_files=self.keep_zip_files,
                file_type='daily',
                execute_serial=self.no_parallel_processing))

            # index daily zip files
            process_list.append(ReportParquetIndexerProcess(
                db_dir=self.db_dir,
                parquet_dir=self.parquet_dir,
                file_type='daily',
                execute_serial=self.no_parallel_processing))

        else:
            print(
                "No rapid-api-key is set: \n"
                + "If you are interested in receiving the latest SEC filings daily and not just "
                + "each quarter, please have a look at "
                + "https://rapidapi.com/hansjoerg.wingeier/api/daily-sec-financial-statement-dataset")  # pylint: disable=C0301

        return process_list

    def _load_post_update_process(self) -> List[AbstractProcess]:
        import importlib  # pylint: disable=C0415

        if not self.post_update_processes:
            return []

        module_str, function_str = self.post_update_processes.rsplit('.', 1)
        module = importlib.import_module(module_str)
        post_processes_function = getattr(module, function_str)

        post_processes = post_processes_function(self.config)

        if not isinstance(post_processes, list):
            LOGGER.error("A post_update_processes function was defined in the configuration: %s"
                         ". But it does not return a list.", self.post_update_processes)
            raise ValueError("Defined post_update_processes function does not return a list: "
                             f"{self.post_update_processes}")

        if not all(isinstance(item, AbstractProcess) for item in post_processes):
            LOGGER.error("A post_update_processes function was defined in the configuration: %s"
                         ". But it not all elements are of type AbstractProcess.",
                         self.post_update_processes)
            raise ValueError("Defined post_update_processes function does have elements that are "
                             f"not of type AbstractPcoess: {self.post_update_processes}")

        LOGGER.info("Loading %d post update processes from %s",
                    len(post_processes), self.post_update_processes)

        return post_processes

    def _execute_post_update_hook(self):
        import importlib  # pylint: disable=C0415

        if not self.post_update_hook:
            return

        module_str, function_str = self.post_update_hook.rsplit('.', 1)
        module = importlib.import_module(module_str)
        post_update_hook = getattr(module, function_str)

        LOGGER.info("Calling post_update_hook %s", self.post_update_hook)
        post_update_hook(self.config)

    def _update(self):
        processes: List[AbstractProcess] = self._build_process_list()
        processes.extend(self._load_post_update_process())
        execute_processes(processes)
        self._execute_post_update_hook()

    def check_for_new_format_quaterfiles(self):
        """
        check if new incompatible datasets with segments column inside num.txt were downloaded.
        Returns:

        """

        zipdirs = get_directories_in_directory(f"{self.parquet_dir}/quarter")
        with_segments: List[str] = []
        for zipdir in zipdirs:
            num_path = Path(self.parquet_dir) / "quarter" / zipdir / "num.txt.parquet"
            columns = fastparquet.ParquetFile(num_path).columns
            if "segments" in columns:
                with_segments.append(zipdir)

        if len(with_segments) > 0:
            LOGGER.info("-------------- ATTENTION -----------------------")
            LOGGER.info("Found downloaded data with new 'segment' column in num.txt dataframes.")
            LOGGER.info("This is not yet supported in this version 1.8.x of the framework and ")
            LOGGER.info("using this data leads to wrong results.")
            LOGGER.info("                 ----                        ")
            LOGGER.info("Please delete all the content in:            ")
            LOGGER.info("- %s", self.db_dir)
            LOGGER.info("- %s", self.dld_dir)
            LOGGER.info("- %s", self.parquet_dir)
            LOGGER.info("                 ----                        ")
            LOGGER.info("After that, you can start again and only compatible versions of the data ")
            LOGGER.info("will be downloaded. ")
            LOGGER.info("                 ----                        ")
            LOGGER.info("A new version of the framework supporting the 'segment' column")
            LOGGER.info("is in work and should be available in February 2025.")
            sys.exit(1)

    def update(self, force_update: bool = False):
        """
        execute the updated process if time has come to check for new upates.
        Returns:

        """

        self.check_for_new_format_quaterfiles()

        if force_update | self.auto_update:
            if not force_update & self.auto_update:
                LOGGER.debug('AutoUpdate is True, so check if new zip files are available')

            if (not force_update) & (not self._check_for_update()):
                LOGGER.debug(
                    'Skipping update: last check was done less than %d seconds ago',
                    Updater.CHECK_EVERY_SECONDS)
                return

            LOGGER.info('Launching data update process ...')
            # create db if necessary
            DbCreator(db_dir=self.db_dir).create_db()

            # execute the update logic
            self._update()

            # update the timestamp of the last check
            self.db_state_accesor.set_key(Updater.LAST_UPDATE_CHECK_KEY, str(time.time()))
