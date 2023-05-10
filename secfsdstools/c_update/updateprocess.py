"""
this module contains the update logic. This means downloading new zipfiles, transforming the data
into parquet format, and indexing the reports.
"""
import logging
import time
from typing import Optional

from secfsdstools.a_utils.dbutils import DBStateAcessor
from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.rapiddownloadutils import RapidUrlBuilder
from secfsdstools.c_download.rapiddownloading import RapidZipDownloader
from secfsdstools.c_download.secdownloading import SecZipDownloader
from secfsdstools.c_index.indexing import ReportParquetIndexer
from secfsdstools.c_transform.toparquettransforming import ToParquetTransformer

LOGGER = logging.getLogger(__name__)


class Updater:
    """Manages the update process: download zipfiles, transform to parquet, and index the reports"""
    LAST_UPDATE_CHECK_KEY: str = 'LAST_UPDATED'
    CHECK_EVERY_SECONDS: int = 24 * 60 * 60  # check every 24 hours

    def __init__(self,
                 db_dir: str,
                 dld_dir: str,
                 daily_dld_dir: str,
                 parquet_dir: str,
                 user_agent: str,
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

    def _check_for_update(self) -> bool:
        """checks if a new update check should be conducted."""
        last_check = self.db_state_accesor.get_key(Updater.LAST_UPDATE_CHECK_KEY)

        if last_check is None:
            return True

        return int(last_check) + Updater.CHECK_EVERY_SECONDS < time.time()

    def _do_download(self):
        urldownloader = UrlDownloader(user_agent=self.user_agent)

        # download data from sec
        LOGGER.info("check if there are new files to download from sec.gov ...")
        secdownloader = SecZipDownloader(zip_dir=self.dld_dir,
                                         parquet_root_dir=self.parquet_dir,
                                         urldownloader=urldownloader)
        secdownloader.download()

        # download data from rapid
        if (self.rapid_api_key is not None) & (self.rapid_api_key != ''):
            try:
                LOGGER.info("check if there are new files to download from rapid...")
                rapidurlbuilder = RapidUrlBuilder(rapid_plan=self.rapid_api_plan,
                                                  rapid_api_key=self.rapid_api_key)
                rapiddownloader = RapidZipDownloader(rapidurlbuilder=rapidurlbuilder,
                                                     daily_zip_dir=self.daily_dld_dir,
                                                     qrtr_zip_dir=self.dld_dir,
                                                     urldownloader=urldownloader,
                                                     parquet_root_dir=self.parquet_dir)
                rapiddownloader.download()
            except Exception as ex:  # pylint: disable=W0703
                LOGGER.warning("Failed to get data from rapid api, please check rapid-api-key. ")
                LOGGER.warning("Only using data from Sec.gov because of: %s", ex)
        else:
            print("No rapid-api-key is set: \n"
                  + "If you are interested in daily updates, please have a look at "
                  + "https://rapidapi.com/hansjoerg.wingeier/api/daily-sec-financial-statement-dataset")

    def _do_transform(self):
        LOGGER.info("start to transform to parquet format ...")
        qrtr_transformer = ToParquetTransformer(zip_dir=self.dld_dir,
                                                parquet_dir=self.parquet_dir,
                                                file_type='quarter')
        qrtr_transformer.process()

        daily_transformer = ToParquetTransformer(zip_dir=self.daily_dld_dir,
                                                 parquet_dir=self.parquet_dir,
                                                 file_type='daily')
        daily_transformer.process()

    def _do_index(self):
        # create parquet index
        LOGGER.info("start to index parquet files ...")
        qrtr_parquet_indexer = ReportParquetIndexer(db_dir=self.db_dir,
                                                    parquet_dir=self.parquet_dir,
                                                    file_type='quarter')
        qrtr_parquet_indexer.process()

        daily_parquet_indexer = ReportParquetIndexer(db_dir=self.db_dir,
                                                     parquet_dir=self.parquet_dir,
                                                     file_type='daily')
        daily_parquet_indexer.process()

    def _update(self):
        self._do_download()
        self._do_transform()
        self._do_index()

    def update(self):

        if not self._check_for_update():
            LOGGER.info('Skipping update check since last check was done less than 24 hours ago')
            return

        # execute the update logic
        self._update()

        # update the timestamp of the last check
        self.db_state_accesor.set_key(Updater.LAST_UPDATE_CHECK_KEY, str(int(time.time())))
