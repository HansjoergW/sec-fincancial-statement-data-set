"""
Manage the configuration
"""
import configparser
import logging
import os
import pprint
import re
from typing import List, Optional

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.dbutils import DBStateAcessor
from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.a_utils.rapiddownloadutils import RapidUrlBuilder
from secfsdstools.c_update.updateprocess import Updater

DEFAULT_CONFIG_FILE: str = '.secfsdstools.cfg'
SECFSDSTOOLS_ENV_VAR_NAME: str = 'SECFSDSTOOLS_CFG'

LOGGER = logging.getLogger(__name__)

DEFAULT_CONFIGURATION = Configuration(
    download_dir=os.path.join(os.path.expanduser('~'), 'secfsdstools/data/dld'),
    db_dir=os.path.join(os.path.expanduser('~'), 'secfsdstools/data/db'),
    parquet_dir=os.path.join(os.path.expanduser('~'), 'secfsdstools/data/parquet'),
    user_agent_email='your.email@goeshere.com',
    auto_update=True,
    keep_zip_files=False,
    no_parallel_processing=False
)

DEFAULT_COMMENTED_LINES = """# If you want to add additional processing steps being
# automatically executed when new data is available, just uncomment the following 
# config lines.
# For more details about these additional steps, have a look at the 
# 08_00_automation_basics notebook.

# postupdateprocesses=secfsdstools.x_examples.automation.automation.define_extra_processes

# [Filter]
# filtered_dir_by_stmt_joined = secfsdstools/pipeline/_1_filtered_by_stmt_joined
 
# [Concat]
# concat_dir_by_stmt_joined = secfsdstools/pipeline/_2_concat_by_stmt_joined

# [Standardizer]
# standardized_dir = secfsdstools/pipeline/_3_standardized

# Note: this is an optional step which needs a significant amount of memory. Leave it commented
# in case of memory problems.
# [SingleBag]
# singlebag_dir = secfsdstools/pipeline/_4_single_bag
"""

class ConfigurationManager:
    """
    Configuration Manager. Reads the configuration from the configuration file.
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
            LOGGER.info('Config file set by environment variable %s to %s',
                        SECFSDSTOOLS_ENV_VAR_NAME, env_config_file)
            if not os.path.isfile(env_config_file):
                LOGGER.error('environment variable %s was set.', SECFSDSTOOLS_ENV_VAR_NAME)
                LOGGER.error('But configuration file is not present, creating it ...')
                conf_dir, _ = os.path.split(env_config_file)
                os.makedirs(conf_dir, exist_ok=True)
                ConfigurationManager._write_configuration(env_config_file, DEFAULT_CONFIGURATION)
                LOGGER.error('configuration file created at %s.', env_config_file)
                LOGGER.error('please check the content ant then restart')
                return ConfigurationManager._handle_first_start(env_config_file,
                                                                DEFAULT_CONFIGURATION)

            return ConfigurationManager._read_configuration(env_config_file)

        current_cfg_file_path = os.path.join(os.getcwd(), DEFAULT_CONFIG_FILE)
        if os.path.isfile(current_cfg_file_path):
            LOGGER.info('found configuration file at %s', current_cfg_file_path)
            return ConfigurationManager._read_configuration(current_cfg_file_path)

            # check if file exists at home directory
        home_cfg_file_path = os.path.join(os.path.expanduser('~'), DEFAULT_CONFIG_FILE)
        if not os.path.isfile(home_cfg_file_path):
            LOGGER.error('No configuration file found at home directory %s.', home_cfg_file_path)
            ConfigurationManager._write_configuration(home_cfg_file_path, DEFAULT_CONFIGURATION)
            LOGGER.error('Config file created at %s. Please check the content and then rerun.',
                         home_cfg_file_path)
            return ConfigurationManager._handle_first_start(home_cfg_file_path,
                                                            DEFAULT_CONFIGURATION)

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
        print(' If you want to change the settings, you can exit, change the settings ')
        print(' and restart. This will trigger the initial update process as described.')
        print(' below.')
        print(' If you are ok with the settings, you can directly press y or Y and the initial ')
        print(' update process will start immediately. \n')
        print(' The initial update process executes the following steps: ')
        print(' 1. It downloads all available quarterly zip files from ' +
              'https://www.sec.gov/dera/data/financial-statement-data-sets.html')
        print(f'    into the folder {config.download_dir}')
        print(' 2. It will transform the CSV files inside the zipfiles into parquet format and')
        print(f'    store them under {config.parquet_dir}')
        print(' 3. The original zip files will be deleted depending on your configuration')
        print(' 4. The content of the SUB.TXT parquet files will be indexed in a simple ')
        print(f'    sqlite database file (placed at {config.db_dir})')
        print('')
        print(' Note: The downloaded data will use about 2GB on your disk, the size doubles, if ')
        print('       you choose to keep the ZIP-files after transformation. ')
        print(' Note: The initial update process will take several minutes.')
        print('')

        inputvalue = input(' Start initial update process [y]/n:')

        if inputvalue in ['Y', 'y', '']:
            ConfigurationManager._do_initial_update(config)
            return config

        print(f'Please check the configuration at {file_path}.')
        raise ValueError(
            f'Please check the configuration at {file_path} and restart. ')

    @staticmethod
    def _do_initial_update(config: Configuration):
        print('start initial report download process')
        updater = Updater.get_instance(config)
        updater.update()


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

        secconfig = Configuration(
            download_dir=config['DEFAULT'].get('DownloadDirectory', ),
            db_dir=config['DEFAULT'].get('DbDirectory'),
            parquet_dir=config['DEFAULT'].get('ParquetDirectory'),
            user_agent_email=config['DEFAULT'].get('UserAgentEmail'),
            rapid_api_key=config['DEFAULT'].get('RapidApiKey', None),
            rapid_api_plan=config['DEFAULT'].get('RapidApiPlan', 'basic'),
            auto_update=config['DEFAULT'].getboolean('AutoUpdate', True),
            keep_zip_files=config['DEFAULT'].getboolean('KeepZipFiles', False),
            no_parallel_processing=config['DEFAULT'].getboolean('NoParallelProcessing', False),
            post_update_hook=config['DEFAULT'].get('PostUpdateHook', None),
            post_update_processes=config['DEFAULT'].get('PostUpdateProcesses', None),
            config_parser=config
        )

        check_messages = ConfigurationManager.check_basic_configuration(secconfig)
        if len(check_messages) > 0:
            print(
                f"""There are problems with your configuration.
                    Please fix the following issues in {file_path}: {check_messages}""")
            raise ValueError(f'Problems with configuration in {file_path}: {check_messages}')

        check_rapid_messages = ConfigurationManager.check_rapid_configuration(secconfig)
        if len(check_rapid_messages) > 0:
            print(f'rapid configuration is invalid in {file_path}: {check_rapid_messages}')
            print('rapid configuration will be ignored.')

            LOGGER.warning('rapid configuration is invalid in %s: %s',
                           file_path, str(check_rapid_messages))
            secconfig.rapid_api_key = None
            secconfig.rapid_api_plan = None
        return secconfig

    @staticmethod
    def _is_valid_email(email):
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

    @staticmethod
    def _validate_post_update_hook(function_name: Optional[str]) -> List[str]:
        if function_name is None:
            return []

        import importlib # pylint: disable=C0415
        import inspect # pylint: disable=C0415

        messages: List[str] = []
        module_str = ""
        function_str = ""
        try:
            module_str, function_str = function_name.rsplit('.', 1)
            module = importlib.import_module(module_str)
            my_function = getattr(module, function_str)
            signature = inspect.signature(my_function)
            params = list(signature.parameters.values())

            if len(params) != 1:
                messages.append(f"Exactly one parameter is expected for {function_name}")

            if not ((params[0].annotation == inspect._empty) | # pylint: disable=W0212
                    (params[0].annotation == Configuration)):
                messages.append(f"Parameter of function has wrong type {params[0].annotation}. "
                                "It should be secfsdstools.a_config.configmodel.Configuration")

        except ModuleNotFoundError:
            messages.append(f"Module {module_str} not found")
        except AttributeError:
            messages.append(f"Function {function_str} not found in module {module_str}")

        if len(messages) > 0:
            messages = ["Definition of PostUpdateHook function has problems: "] + messages

        return messages

    @staticmethod
    def _validate_post_update_processes(function_name: Optional[str]) -> List[str]:
        if function_name is None:
            return []

        import importlib # pylint: disable=C0415
        import inspect # pylint: disable=C0415
        from secfsdstools.c_automation.task_framework import AbstractProcess # pylint: disable=C0415

        messages: List[str] = []
        module_str = ""
        function_str = ""
        try:
            module_str, function_str = function_name.rsplit('.', 1)
            module = importlib.import_module(module_str)
            my_function = getattr(module, function_str)
            signature = inspect.signature(my_function)
            params = list(signature.parameters.values())

            if len(params) != 1:
                messages.append(f"Expecting exactly one parameter is expected for {function_name}")

            if not ((params[0].annotation == inspect._empty) | # pylint: disable=W0212
                    (params[0].annotation == Configuration)):
                messages.append(f"Parameter of function has wrong type {params[0].annotation}. "
                                "It should be secfsdstools.a_config.configmodel.Configuration")

            if not ((signature.return_annotation == inspect._empty) | # pylint: disable=W0212
                    (signature.return_annotation == List[AbstractProcess])):
                messages.append(
                    f"Return type of function has wrong type {signature.return_annotation}. "
                    "It should be List[secfsdstools.c_automation.task_framework.AbstractProcess]")

        except ModuleNotFoundError:
            messages.append(f"Module {module_str} not found")
        except AttributeError:
            messages.append(f"Function {function_str} not found in module {module_str}")

        if len(messages) > 0:
            messages = ["Definition of PostUpdateProcesses function has problems: "] + messages

        return messages

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

        messages.extend(ConfigurationManager._validate_post_update_hook(config.post_update_hook))

        messages.extend(
            ConfigurationManager._validate_post_update_processes(config.post_update_processes))

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
                             'AutoUpdate': configuration.auto_update,
                             'KeepZipFiles': configuration.keep_zip_files,
                             'NoParallelProcessing': configuration.no_parallel_processing}



        with open(file_path, 'w', encoding="utf8") as configfile:
            config.write(configfile)
            configfile.write(DEFAULT_COMMENTED_LINES)
