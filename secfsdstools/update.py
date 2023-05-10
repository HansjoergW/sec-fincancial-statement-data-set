""" Getting everything ready to work with the data. """
import logging

from secfsdstools.a_config.configmgt import ConfigurationManager, Configuration
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_download.rapiddownloading import RapidZipDownloader
from secfsdstools.c_download.secdownloading import SecZipDownloader
from secfsdstools.c_transform.toparquettransforming import ToParquetTransformer
from secfsdstools.c_index.indexing import ReportParquetIndexer
from secfsdstools.c_index.indexing import ReportZipIndexer
from secfsdstools.c_update.updateprocess import Updater

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
    updater = Updater(
        db_dir=config.db_dir,
        dld_dir=config.download_dir,
        daily_dld_dir=config.daily_download_dir,
        parquet_dir=config.parquet_dir,
        user_agent=config.user_agent_email,
        rapid_api_key=config.rapid_api_key,
        rapid_api_plan=config.rapid_api_plan
    )
    updater.update()


if __name__ == '__main__':
    update()
    # Configuration(
    #     db_dir='c:/ieu/projects/sec-fincancial-statement-data-set/data/db/',
    #     download_dir='c:/ieu/projects/sec-fincancial-statement-data-set/data/dld/',
    #     user_agent_email='your.email@goes.here'
    # ))
