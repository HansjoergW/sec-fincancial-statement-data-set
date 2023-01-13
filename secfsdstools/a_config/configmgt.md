Module secfsdstools.a_config.configmgt
======================================
Manage the configuration

Classes
-------

`Configuration(download_dir: str, db_dir: str, user_agent_email: str)`
:   Basic configuration settings

    ### Class variables

    `db_dir: str`
    :

    `download_dir: str`
    :

    `user_agent_email: str`
    :

`ConfigurationManager()`
:   Configuration Manager. Reads the configuration from the config file.
    If the file does not exist, it will create one in the current directory

    ### Static methods

    `read_config_file() ‑> secfsdstools.a_config.configmgt.Configuration`
    :   reads the configuration object:
        1. checks there is a set environment variable
        2. checks if it is at the current working  directory
        3. checks if it is in the user home
        :return: Configuration instance