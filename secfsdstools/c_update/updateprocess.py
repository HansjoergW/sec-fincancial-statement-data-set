"""
this module contains the update logic. This means downloading new zipfiles, transforming the data
into parquet format, and indexing the reports.
"""
import logging
import random
import sys
import time
from pathlib import Path
from typing import List

import fastparquet

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.dbutils import DBStateAcessor
from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_automation.task_framework import AbstractProcess, execute_processes
from secfsdstools.c_download.secdownloading_process import SecDownloadingProcess
from secfsdstools.c_index.indexing_process import ReportParquetIndexerProcess
from secfsdstools.c_transform.toparquettransforming_process import ToParquetTransformerProcess

LOGGER = logging.getLogger(__name__)

sponsor_messages = [
    "Enjoying secfsdstools? Please consider sponsoring the project!",
    "Love the tool? Your support keeps development alive – consider sponsoring!",
    "If you find this tool useful, a sponsorship would be greatly appreciated!",
    "Help us continue to improve secfsdstools by becoming a sponsor!",
    "Support open source: Sponsor secfsdstools today!",
    "Keep the updates coming – sponsor secfsdstools and fuel further development.",
    "Like what you see? Consider sponsoring to help drive innovation.",
    "Your support makes a difference. Please sponsor secfsdstools!",
    "Sponsor secfsdstools and help us build a better tool for everyone.",
    "Support innovation and open source by sponsoring secfsdstools.",
    "Your sponsorship ensures continued updates. Thank you for your support!",
    "Help us keep secfsdstools running smoothly – your sponsorship matters.",
    "If you value this tool, your sponsorship is a great way to contribute!",
    "Support the developer behind secfsdstools – consider sponsoring today.",
    "Enjoy the convenience? Sponsor secfsdstools and help us grow.",
    "Be a champion for open source – sponsor secfsdstools and support innovation."
]


def print_sponsoring_message():
    """ create sponsoring message """

    message = random.choice(sponsor_messages)

    # ANSI-Escape-Codes für Farben und Formatierungen
    reset = "\033[0m"
    bold = "\033[1m"
    yellow = "\033[33m"
    white = "\033[37m"

    # Rahmen um die Nachricht erzeugen
    border = "-" * (len(message) + 8)
    hash_border = "#" * (len(message) + 8)

    # Präsentation des Sponsor-Hinweises mit Farben und Hervorhebung
    print("\n\n")
    print(yellow + border + reset)
    print(bold + yellow + hash_border + reset)
    print("\n")
    print(bold + white + "    " + message + "    " + reset)
    print("\n")
    print(bold + white + "    https://github.com/sponsors/HansjoergW" + reset)
    print("\n")
    print(bold + yellow + hash_border + reset)
    print(yellow + border + reset)
    print("\n\n")


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

    def check_for_old_format_quaterfiles(self):
        """
        check if old incompatible datasets without segments column inside num.txt were downloaded.
        """

        zipdirs = get_directories_in_directory(f"{self.parquet_dir}/quarter")
        without_segments: List[str] = []
        for zipdir in zipdirs:
            num_path = Path(self.parquet_dir) / "quarter" / zipdir / "num.txt.parquet"
            columns = fastparquet.ParquetFile(num_path).columns
            if "segments" not in columns:
                without_segments.append(zipdir)

        if len(without_segments) > 0:
            LOGGER.info("-------------- ATTENTION -----------------------")
            LOGGER.info("Found downloaded datasets without new 'segments' column in num.txt")
            LOGGER.info("dataframes. This is not supported anymore in this version of the ")
            LOGGER.info("framework. If you want to use the old, smaller datasets which are ")
            LOGGER.info(
                "still available at https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets-archive")  # pylint: disable=C0301
            LOGGER.info("you have to use version 1.8.2 of the framework.")
            LOGGER.info("                 ----                        ")
            LOGGER.info("If you want to use the datasets with the segments column, you have to ")
            LOGGER.info("delete the content in: ")
            LOGGER.info("- %s", self.db_dir)
            LOGGER.info("- %s", self.dld_dir)
            LOGGER.info("- %s", self.parquet_dir)
            LOGGER.info("                 ----                        ")
            LOGGER.info("After that, you can run again and only compatible versions of the data ")
            LOGGER.info("will be downloaded. ")
            LOGGER.info("                 ----                        ")
            sys.exit(1)

    def update(self, force_update: bool = False):
        """
        execute the updated process if time has come to check for new upates.
        Returns:
        """

        self.check_for_old_format_quaterfiles()

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

            print_sponsoring_message()

            # update the timestamp of the last check
            self.db_state_accesor.set_key(Updater.LAST_UPDATE_CHECK_KEY, str(time.time()))
