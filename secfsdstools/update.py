""" Getting everything ready to work with the data. """
import logging

from secfsdstools.a_config.configmgt import ConfigurationManager, Configuration
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_download.rapiddownloading import RapidZipDownloader
from secfsdstools.c_download.secdownloading import SecZipDownloader
from secfsdstools.d_index.indexing import ReportZipIndexer

LOGGER = logging.getLogger(__name__)


def update(config: Configuration = None):
    """
    ensures that all available zip files are downloaded and that the index is created.
    """
    # check if a logger is active if not, make sure it logs to the console
    if len(logging.root.handlers) == 0:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
            handlers=[
                logging.StreamHandler()
            ]
        )

    # read config
    if config is None:
        config = ConfigurationManager.read_config_file()

    # create the db
    DbCreator(db_dir=config.db_dir).create_db()

    # download data from sec.gov
    LOGGER.info("check if there are new files to download from sec.gov ...")
    secdownloader = SecZipDownloader.get_downloader(configuration=config)
    secdownloader.download()

    # download data from rapid
    if (config.rapid_api_key is not None) & (config.rapid_api_key != ''):
        try:
            LOGGER.info("check if there are new files to download from rapid...")
            rapiddownloader = RapidZipDownloader.get_downloader(configuration=config)
            rapiddownloader.download()
        except Exception as ex: # pylint: disable=W0703
            LOGGER.warning("Failed to get data from rapid api, please check rapid-api-key. ")
            LOGGER.warning("Only using data from Sec.gov because of: %s", ex)
    else:
        print("No rapid-api-key is set: \n"
              + "If you are interested in daily updates, please have a look at "
              + "https://rapidapi.com/hansjoerg.wingeier/api/daily-sec-financial-statement-dataset")

    # create index of reports
    LOGGER.info("start to index downloaded files ...")
    qrtr_indexer = ReportZipIndexer(db_dir=config.db_dir,
                                    zip_dir=config.download_dir,
                                    file_type='quarter')
    qrtr_indexer.process()

    daily_indexer = ReportZipIndexer(db_dir=config.db_dir,
                                     zip_dir=config.daily_download_dir,
                                     file_type='daily')
    daily_indexer.process()


if __name__ == '__main__':
    update()
    # Configuration(
    #     db_dir='c:/ieu/projects/sec-fincancial-statement-data-set/data/db/',
    #     download_dir='c:/ieu/projects/sec-fincancial-statement-data-set/data/dld/',
    #     user_agent_email='your.email@goes.here'
    # ))
