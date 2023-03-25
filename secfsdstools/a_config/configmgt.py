"""
Manage the configuration
"""
import configparser
import logging
import os
import re
from dataclasses import dataclass
from typing import Optional, List

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.rapiddownloadutils import RapidUrlBuilder

DEFAULT_CONFIG_FILE: str = '.secfsdstools.cfg'
SECFSDSTOOLS_ENV_VAR_NAME: str = 'SECFSDSTOOLS_CFG'

LOGGER = logging.getLogger(__name__)


@dataclass
class Configuration:
    """ Basic configuration settings """
    download_dir: str
    db_dir: str
    user_agent_email: str
    rapid_api_key: Optional[str] = None
    rapid_api_plan: Optional[str] = 'basic'
    daily_download_dir: Optional[str] = None

    def __post_init__(self):
        self.daily_download_dir = os.path.join(self.download_dir, "daily")


DEFAULT_CONFIGURATION = Configuration(
    download_dir=os.path.join(os.path.expanduser('~'), 'secfsdstools/data/dld'),
    db_dir=os.path.join(os.path.expanduser('~'), 'secfsdstools/data/db'),
    user_agent_email='your.email@goeshere.com'
)


class ConfigurationManager:
    """
    Configuration Manager. Reads the configuration from the config file.
    If the file does not exist, it will create one in the current directory
    """

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
                raise ValueError(
                    f'environment variable {SECFSDSTOOLS_ENV_VAR_NAME}' +
                    ' was set but config file was not present. ' +
                    f'It was created at location {env_config_file}. Please check it and rerun')

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
            raise ValueError(
                'Config file not found at user home directory. ' +
                f'It was created at location {home_cfg_file_path}. Please check content and rerun')
        return ConfigurationManager._read_configuration(home_cfg_file_path)

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
            user_agent_email=config['DEFAULT'].get('UserAgentEmail'),
            rapid_api_key=config['DEFAULT'].get('RapidApiKey', None),
            rapid_api_plan=config['DEFAULT'].get('RapidApiPlan', 'basic')
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
            try:
                rapidurlbuilder = RapidUrlBuilder(rapid_api_key=config.rapid_api_key,
                                                  rapid_plan='basic')
                UrlDownloader(config.user_agent_email).get_url_content(
                    url=rapidurlbuilder.get_heartbeat_url(),
                    headers=rapidurlbuilder.get_headers(),
                    max_tries=2
                )
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
                             'UserAgentEmail': configuration.user_agent_email}
        with open(file_path, 'w', encoding="utf8") as configfile:
            config.write(configfile)
