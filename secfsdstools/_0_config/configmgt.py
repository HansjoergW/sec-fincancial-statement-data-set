"""
Manage the configuration
"""
import configparser
import logging
import os
from dataclasses import dataclass

DEFAULT_CONFIG_FILE: str = '.secfsdstools.cfg'
SECFSDSTOOLS_ENV_VAR_NAME: str = 'SECFSDSTOOLS_CFG'

LOGGER = logging.getLogger(__name__)


@dataclass
class Configuration:
    """ Basic configuration settings """
    download_dir: str
    db_dir: str
    user_agent_email: str


DEFAULT_CONFIGURATION = Configuration(
    download_dir=os.path.join(os.path.expanduser('~'), '/data/dld'),
    db_dir=os.path.join(os.path.expanduser('~'), './data/db'),
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
        :return: Configuration instance
        """
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
            LOGGER.error('found no config file at home directory %s', home_cfg_file_path)
            ConfigurationManager._write_configuration(home_cfg_file_path, DEFAULT_CONFIGURATION)
            LOGGER.error('config file created at %s. please check the content ant then restart',
                         home_cfg_file_path)
            raise ValueError(
                'Config file not found at user home directory. ' +
                f'It was created at location {home_cfg_file_path}. Please check it and rerun')
        return ConfigurationManager._read_configuration(home_cfg_file_path)

    @staticmethod
    def _read_configuration(file_path: str) -> Configuration:
        """
        Read the configuration file.
        :return: Configuration data class
        """
        config = configparser.ConfigParser()
        config.read(file_path)

        return Configuration(
            download_dir=config['DEFAULT'].get('DownloadDirectory', ),
            db_dir=config['DEFAULT'].get('DbDirectory'),
            user_agent_email=config['DEFAULT'].get('UserAgentEmail')
        )

    @staticmethod
    def _write_configuration(file_path: str, configuration: Configuration):
        """
        Write the configuration to the configured file.
        :param configuration: Configuration instance
        """
        config = configparser.ConfigParser()
        config['DEFAULT'] = {'DownloadDirectory': configuration.download_dir,
                             'DbDirectory': configuration.db_dir,
                             'UserAgentEmail': configuration.user_agent_email}
        with open(file_path, 'w', encoding="utf8") as configfile:
            config.write(configfile)
