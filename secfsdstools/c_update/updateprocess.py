"""
this module contains the update logic. This means downloading new zipfiles, transforming the data
into parquet format, and indexing the reports.
"""
import logging
import time
from typing import Optional, List

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.dbutils import DBStateAcessor
from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.rapiddownloadutils import RapidUrlBuilder
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_automation.task_framework import AbstractProcess
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
            Updater: the instanc

        """
        return Updater(
            db_dir=config.db_dir,
            dld_dir=config.download_dir,
            daily_dld_dir=config.daily_download_dir,
            parquet_dir=config.parquet_dir,
            user_agent=config.user_agent_email,
            keep_zip_files=config.keep_zip_files,
            auto_update=config.auto_update,
            rapid_api_key=config.rapid_api_key,
            rapid_api_plan=config.rapid_api_plan
        )

    def __init__(self,
                 db_dir: str,
                 dld_dir: str,
                 daily_dld_dir: str,
                 parquet_dir: str,
                 user_agent: str,
                 keep_zip_files: bool,
                 auto_update: bool,
                 rapid_api_plan: Optional[str],
                 rapid_api_key: Optional[str]):
        self.db_state_accesor = DBStateAcessor(db_dir=db_dir)
        self.db_dir = db_dir
        self.dld_dir = dld_dir
        self.daily_dld_dir = daily_dld_dir
        self.parquet_dir = parquet_dir
        self.user_agent = user_agent
        self.rapid_api_plan = rapid_api_plan
        self.rapid_api_key = rapid_api_key
        self.keep_zip_files = keep_zip_files
        self.auto_update = auto_update

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
                                                  urldownloader=urldownloader))
        # transform sec zip files
        process_list.append(ToParquetTransformerProcess(zip_dir=self.dld_dir,
                                                        parquet_dir=self.parquet_dir,
                                                        keep_zip_files=self.keep_zip_files,
                                                        file_type='quarter'))

        # index sec zip files
        process_list.append(ReportParquetIndexerProcess(db_dir=self.db_dir,
                                                        parquet_dir=self.parquet_dir,
                                                        file_type='quarter'))

        if (self.rapid_api_key is not None) & (self.rapid_api_key != ''):
            # download daily zip files
            rapidurlbuilder = RapidUrlBuilder(rapid_plan=self.rapid_api_plan,
                                              rapid_api_key=self.rapid_api_key)
            process_list.append(RapidDownloadingProcess(rapidurlbuilder=rapidurlbuilder,
                                                        daily_zip_dir=self.daily_dld_dir,
                                                        qrtr_zip_dir=self.dld_dir,
                                                        urldownloader=urldownloader,
                                                        parquet_root_dir=self.parquet_dir))

            # transform daily zip files
            process_list.append(ToParquetTransformerProcess(zip_dir=self.daily_dld_dir,
                                                            parquet_dir=self.parquet_dir,
                                                            keep_zip_files=self.keep_zip_files,
                                                            file_type='daily'))

            # index daily zip files
            process_list.append(ReportParquetIndexerProcess(db_dir=self.db_dir,
                                                            parquet_dir=self.parquet_dir,
                                                            file_type='daily'))

        else:
            print("No rapid-api-key is set: \n"
                  + "If you are interested in daily updates, please have a look at "
                  + "https://rapidapi.com/hansjoerg.wingeier/api/daily-sec-financial-statement-dataset")

        return process_list

    def _update(self):
        processes: List[AbstractProcess] = self._build_process_list()

        for process in processes:
            process.process()

    def update(self, force_update: bool = False):
        """
        execute the updated process if time has come to check for new upates.
        Returns:

        """
        if force_update | self.auto_update:
            print("------------------ UPDATE CALLED -----------------------")
            if not force_update & self.auto_update:
                LOGGER.debug('AutoUpdate is True, so check if new zip files are available')

            if not self._check_for_update():
                LOGGER.debug(
                    'Skipping update: last check was done less than %d seconds ago',
                    Updater.CHECK_EVERY_SECONDS)
                return

            LOGGER.info('Check if new report zip files are available...')
            # create db if necessary
            DbCreator(db_dir=self.db_dir).create_db()

            # execute the update logic
            self._update()

            # update the timestamp of the last check
            self.db_state_accesor.set_key(Updater.LAST_UPDATE_CHECK_KEY, str(time.time()))
