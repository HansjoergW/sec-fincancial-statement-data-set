"""
Manage the configuration
"""
import configparser
import logging
import os
import pprint
import re
import sys
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional, List

from secfsdstools.a_utils.dbutils import DBStateAcessor
from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.rapiddownloadutils import RapidUrlBuilder

DEFAULT_CONFIG_FILE: str = '.secfsdstools.cfg'
SECFSDSTOOLS_ENV_VAR_NAME: str = 'SECFSDSTOOLS_CFG'

LOGGER = logging.getLogger(__name__)


class AccessorType(Enum):
    """
    Defines the AccessType which depends on how the data is stored
    """
    ZIP = 1
    PARQUET = 2


@dataclass
class Configuration:
    """ Basic configuration settings """
    download_dir: str
    db_dir: str
    parquet_dir: str
    user_agent_email: str
    rapid_api_key: Optional[str] = None
    rapid_api_plan: Optional[str] = 'basic'
    daily_download_dir: Optional[str] = None
    use_parquet: Optional[bool] = True

    def __post_init__(self):
        self.daily_download_dir = os.path.join(self.download_dir, "daily")

    def get_accessor_type(self) -> AccessorType:
        """
        returns the access type, depending on how the flag use_parquet is set
        Returns:
            AccessorType: accessor type, whether to use parquet or csv in zip

        """
        return AccessorType.PARQUET if self.use_parquet else AccessorType.ZIP

    def get_dict(self):
        return dict(asdict(self))


DEFAULT_CONFIGURATION = Configuration(
    download_dir=os.path.join(os.path.expanduser('~'), 'secfsdstools/data/dld'),
    db_dir=os.path.join(os.path.expanduser('~'), 'secfsdstools/data/db'),
    parquet_dir=os.path.join(os.path.expanduser('~'), 'secfsdstools/data/parquet'),
    user_agent_email='your.email@goeshere.com',
    use_parquet=True
)


class ConfigurationManager:
    """
    Configuration Manager. Reads the configuration from the config file.
    If the file does not exist, it will create one in the current directory
    """

    SUCCESSFULL_RAPID_API_KEY: str = "RAPID_KEY"

    @staticmethod
    def read_config_file() -> Configuration:
        """
        reads the configuration object:
        1. checks there is a set environment variable
        2. checks if it is at the current working  directory
        3. checks if it is in the user home

        Returns:
            Configuration: configuration instance
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

        env_config_file = os.getenv(SECFSDSTOOLS_ENV_VAR_NAME)
        if env_config_file:
            LOGGER.info('read configuration from %s', env_config_file)
            if not os.path.isfile(env_config_file):
                LOGGER.error('environment variable %s was set.', SECFSDSTOOLS_ENV_VAR_NAME)
                LOGGER.error('But config file is not present, creating it ...')
                conf_dir, _ = os.path.split(env_config_file)
                os.makedirs(conf_dir, exist_ok=True)
                ConfigurationManager._write_configuration(env_config_file, DEFAULT_CONFIGURATION)
                LOGGER.error('config file created at %s.', env_config_file)
                LOGGER.error('please check the content ant then restart')
                ConfigurationManager._handle_first_start(env_config_file, DEFAULT_CONFIGURATION)

            return ConfigurationManager._read_configuration(env_config_file)

        current_cfg_file_path = os.path.join(os.getcwd(), DEFAULT_CONFIG_FILE)
        if os.path.isfile(current_cfg_file_path):
            LOGGER.info('found config file at %s', current_cfg_file_path)
            return ConfigurationManager._read_configuration(current_cfg_file_path)

            # check if file exists at home directory
        home_cfg_file_path = os.path.join(os.path.expanduser('~'), DEFAULT_CONFIG_FILE)
        if not os.path.isfile(home_cfg_file_path):
            LOGGER.error('No config file found at home directory %s.', home_cfg_file_path)
            ConfigurationManager._write_configuration(home_cfg_file_path, DEFAULT_CONFIGURATION)
            LOGGER.error('Config file created at %s. Please check the content and then rerun.',
                         home_cfg_file_path)
            ConfigurationManager._handle_first_start(home_cfg_file_path, DEFAULT_CONFIGURATION)

        return ConfigurationManager._read_configuration(home_cfg_file_path)

    @staticmethod
    def _handle_first_start(file_path: str, config: Configuration):
        print(' --------------------------------------------------------------------------')
        print(' Wellcome!')
        print(' It looks as if this is the first time you are using this package since no ')
        print(f' configuration was found at {file_path}. A default configuration file was ')
        print(' created at that location. It contains fhe following default values: \n')
        pprint.pprint(config.get_dict())
        print('')
        print(' If you want to change the settings, you can exit and and change the settings ')
        print(' and restart. This will then trigger the initial update process as described.')
        print(' below.')
        print(' If you are ok with the settings, you can directly press y or Y and the initial ')
        print(' update process will start immediately. \n')
        print(' The initial update process executes the following steps: ')
        print(' 1. It downloads all available quarterly zip files from ' +
              'https://www.sec.gov/dera/data/financial-statement-data-sets.html')
        print(f'    into the folder {config.download_dir}')
        print(' 2. It will transform the CSV files inside the zipfiles into parquet format and')
        print(f'    store them under {config.parquet_dir}')
        print(' 3. The original zip file will be deleted depending on your configuration')
        print(' 4. The content of the SUB.TXT parquet files will be indexed in a simple ')
        print(f'    sqlite database file (placed at {config.db_dir})')
        print('')
        print(' Note: The downloaded data will use about 2GB on your disk, the size doubles, if ')
        print('        choose to keep the ZIP-files after transformation. ')
        print(' Note: The initial update process will take several minutes.')
        print('')

        inputvalue = input(' Start initial update process [y]/n:')

        if inputvalue in ['Y', 'y', '']:
            print('start initial report download process')
            from secfsdstools.update import update
            update(config)
        else:
            print(f'Please check the configuration at {file_path}.')

            raise ValueError(
                f'Please check the configuration at {file_path} and restart. ')

    @staticmethod
    def _read_configuration(file_path: str) -> Configuration:
        """
        Read the configuration file.

        Returns:
             Configuration: instance
        """
        LOGGER.info('reading configuration from %s', file_path)
        config = configparser.ConfigParser()
        config.read(file_path)

        config = Configuration(
            download_dir=config['DEFAULT'].get('DownloadDirectory', ),
            db_dir=config['DEFAULT'].get('DbDirectory'),
            parquet_dir=config['DEFAULT'].get('ParquetDirectory'),
            user_agent_email=config['DEFAULT'].get('UserAgentEmail'),
            rapid_api_key=config['DEFAULT'].get('RapidApiKey', None),
            rapid_api_plan=config['DEFAULT'].get('RapidApiPlan', 'basic'),
            use_parquet=config['DEFAULT'].getboolean('UserParquet', True)
        )

        check_messages = ConfigurationManager.check_basic_configuration(config)
        if len(check_messages) > 0:
            print(
                f"""There are problems with your configuration.
                    Please fix the following issues in {file_path}: {check_messages}""")
            raise ValueError(f'Problems with configuration in {file_path}: {check_messages}')

        check_rapid_messages = ConfigurationManager.check_rapid_configuration(config)
        if len(check_rapid_messages) > 0:
            print(f'rapid configuration is invalid in {file_path}: {check_rapid_messages}')
            print('rapid configuration will be ignored.')

            LOGGER.warning('rapid configuration is invalid in %s: %s',
                           file_path, str(check_rapid_messages))
            config.rapid_api_key = None
            config.rapid_api_plan = None
        return config

    @staticmethod
    def _is_valid_email(email):
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

    @staticmethod
    def check_basic_configuration(config: Configuration) -> List[str]:
        """
        Validates the basic configuration:

        Args:
            config (Configuration): the configuration to be validated

        Returns:
            List[str]: List with the invalid configurations
        """

        messages: List[str] = []

        if not os.path.isdir(config.db_dir):
            LOGGER.info("SQLite db directory does not exist, creating it at %s", config.db_dir)
            os.makedirs(config.db_dir, exist_ok=True)

        if not os.path.isdir(config.download_dir):
            LOGGER.info("Download directory does not exist, creating it at %s", config.download_dir)
            os.makedirs(config.download_dir, exist_ok=True)

        if not os.path.isdir(config.parquet_dir):
            LOGGER.info("Parquet directory does not exist, creating it at %s", config.parquet_dir)
            os.makedirs(config.parquet_dir, exist_ok=True)
            os.makedirs(os.path.join(config.parquet_dir, 'quarter'), exist_ok=True)
            os.makedirs(os.path.join(config.parquet_dir, 'daily'), exist_ok=True)

        if not ConfigurationManager._is_valid_email(config.user_agent_email):
            messages.append(
                f'The defined UserAgentEmail is not a valid format: {config.user_agent_email}')

        return messages

    @staticmethod
    def check_rapid_configuration(config: Configuration) -> List[str]:
        """
        Validates the rapid configuration:

        Args:
            config (Configuration): the configuration to be validated

        Returns:
            List[str]: List with the invalid configurations
        """
        messages: List[str] = []

        if config.rapid_api_plan not in ['basic', 'premium', None]:
            messages.append(
                f'The defined RapidApiPlan ({config.rapid_api_plan}) is not valid.' +
                ' Allowed values are basic, premium')

        if config.rapid_api_key is not None:

            # only check rapid api key if it was changed or if
            accessor = DBStateAcessor(db_dir=config.db_dir)
            checked_rapid_key = accessor.get_key(ConfigurationManager.SUCCESSFULL_RAPID_API_KEY)

            if checked_rapid_key == config.rapid_api_key:
                return messages

            try:
                rapidurlbuilder = RapidUrlBuilder(rapid_api_key=config.rapid_api_key,
                                                  rapid_plan='basic')
                UrlDownloader(config.user_agent_email).get_url_content(
                    url=rapidurlbuilder.get_heartbeat_url(),
                    headers=rapidurlbuilder.get_headers(),
                    max_tries=2
                )
                # store the key that was successfully tested
                accessor.set_key(ConfigurationManager.SUCCESSFULL_RAPID_API_KEY,
                                 config.rapid_api_key)
            except Exception as err:  # pylint: disable=W0703
                messages.append(f'RapidApiKey {config.rapid_api_key} was set' +
                                f' but seems to be not valid: {str(err)}\n' +
                                'Please go to rapidapi.com and create a valid api key if you' +
                                ' want to have daily data updates')

        return messages

    @staticmethod
    def _write_configuration(file_path: str, configuration: Configuration):
        """
        Write the configuration to the configured file.
        """
        config = configparser.ConfigParser()
        config['DEFAULT'] = {'DownloadDirectory': configuration.download_dir,
                             'DbDirectory': configuration.db_dir,
                             'ParquetDirectory': configuration.parquet_dir,
                             'UserAgentEmail': configuration.user_agent_email,
                             'UseParquet': configuration.use_parquet}
        with open(file_path, 'w', encoding="utf8") as configfile:
            config.write(configfile)


if __name__ == '__main__':
    ConfigurationManager._handle_first_start("my/config/path", DEFAULT_CONFIGURATION)
