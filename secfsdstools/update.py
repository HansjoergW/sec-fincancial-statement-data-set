""" Getting everything ready to work with the data. """
import logging

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
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
    updater = Updater.get_instance(config)
    updater.update()


if __name__ == '__main__':
    update()
    # Configuration(
    #     db_dir='c:/ieu/projects/sec-fincancial-statement-data-set/data/db/',
    #     download_dir='c:/ieu/projects/sec-fincancial-statement-data-set/data/dld/',
    #     user_agent_email='your.email@goes.here'
    # ))
