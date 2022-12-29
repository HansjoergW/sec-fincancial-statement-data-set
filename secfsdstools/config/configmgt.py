"""
Read configuration
"""
import configparser
import logging
import os
from dataclasses import dataclass

DEFAULT_CONFIG_FILE: str = 'secsdfstools.cfg'

LOGGER = logging.getLogger(__name__)


@dataclass
class Configuration:
    """ Basic configuration settings """
    download_dir: str
    create_index: bool
    user_agent_email: str


class ConfigurationManager:
    """
    Configuration Manager. Reads the configuration from the config file.
    If the file does not exist, it will create one in the current directory
    """

    def __init__(self, filename: str = DEFAULT_CONFIG_FILE):
        self.filename = filename
        self.config = None
        if os.path.exists(filename):
            LOGGER.info('Get configuration from %s', self.filename)
            self.config = self.read_configuration()
        else:
            self.config = Configuration(download_dir='./dld', create_index=True,
                                        user_agent_email='your.email@goeshere.com')
            LOGGER.info('Configuration file does not exist - create it as %s', self.filename)
            self.write_configuration(self.config)

    def get_configuration(self) -> Configuration:
        """
        Returns the configuration
        :return: Configuration class isntance
        """
        return self.config

    def read_configuration(self) -> Configuration:
        """
        Read the configuration file.
        :return: Configuration data class
        """
        config = configparser.ConfigParser()
        config.read(self.filename)

        return Configuration(
            download_dir=config['DEFAULT'].get('DownloadDirectory', './dld'),
            create_index=config['DEFAULT'].getboolean('CreateIndex', True),
            user_agent_email=config['DEFAULT'].get('UserAgentEmail', 'your.email@goeshere.com')
        )

    def write_configuration(self, configuration: Configuration):
        """
        Write the configuration to the configured file.
        :param configuration: Configuration instance
        """
        config = configparser.ConfigParser()
        config['DEFAULT'] = {'DownloadDirectory': configuration.download_dir,
                             'CreateIndex': configuration.create_index,
                             'UserAgentEmail': configuration.user_agent_email}
        with open(self.filename, 'w', encoding="utf8") as configfile:
            config.write(configfile)
