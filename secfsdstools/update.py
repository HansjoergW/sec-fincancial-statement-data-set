""" Getting everything ready to work with the data. """
import logging

from secfsdstools.a_config.configmodel import Configuration

LOGGER = logging.getLogger(__name__)


# pylint: disable=C0415
def update(config: Configuration = None, force_update: bool = False):
    """
    ensures that all available zip files are downloaded and that the index is created.
    """
    from secfsdstools.c_update.updateprocess import Updater
    from secfsdstools.a_config.configmgt import ConfigurationManager

    # check if a logger is active if not, make sure it logs at least to the console
    if len(logging.root.handlers) == 0:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
            handlers=[
                logging.StreamHandler()
            ]
        )

    # read configuration
    if config is None:
        config = ConfigurationManager.read_config_file()

    # create the db
    updater = Updater.get_instance(config)
    updater.update(force_update=force_update)


if __name__ == '__main__':
    update(force_update=True)
    # Configuration(
    #     db_dir='c:/ieu/projects/sec-fincancial-statement-data-set/data/db/',
    #     download_dir='c:/ieu/projects/sec-fincancial-statement-data-set/data/dld/',
    #     user_agent_email='your.email@goes.here'
    # ))
