import os
from io import StringIO
from typing import List
from unittest.mock import patch

import pytest

from secfsdstools.a_config.configmgt import ConfigurationManager, SECFSDSTOOLS_ENV_VAR_NAME, \
    DEFAULT_CONFIG_FILE
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_automation.task_framework import AbstractProcess

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
NOT_EXISTING_CFG = f'{CURRENT_DIR}/config.cfg'


def test_environment_variable_no_file(tmp_path, monkeypatch):
    # check if file is created if the env variable is set
    config_file = f'{str(tmp_path)}/test.cfg'
    with patch.dict(os.environ, {SECFSDSTOOLS_ENV_VAR_NAME: config_file}, clear=True):
        # configure command line input for _handle_first_start method
        monkeypatch.setattr('sys.stdin', StringIO('n\n'))

        with pytest.raises(ValueError):
            ConfigurationManager.read_config_file()

            # check if file is present
            env_config_file = os.getenv(SECFSDSTOOLS_ENV_VAR_NAME)
            assert os.path.isfile(env_config_file)


def test_environment_variable_with_file(tmp_path):
    # check if file is read if the env variable is set

    # create the configuration at the expected location
    config_file = f'{str(tmp_path)}/test.cfg'
    ConfigurationManager._write_configuration(
        config_file,
        Configuration(db_dir=os.path.join(tmp_path, 'blublu'),
                      download_dir=os.path.join(tmp_path, 'blublu'),
                      user_agent_email='user@email.com',
                      parquet_dir=os.path.join(tmp_path, 'parquet'),
                      auto_update=False  # prevent calling update
                      ))

    with patch.dict(os.environ, {SECFSDSTOOLS_ENV_VAR_NAME: config_file}, clear=True):
        configuration = ConfigurationManager.read_config_file()
        assert configuration is not None
        assert configuration.db_dir.endswith('blublu')


# Tests
def test_config_file_in_cwd(tmp_path, monkeypatch: pytest.MonkeyPatch):
    # tests if the configuration file is read from the current directory if present

    # patch the current working dir to a temp path
    monkeypatch.chdir(tmp_path)
    config_file = str(tmp_path / DEFAULT_CONFIG_FILE)

    ConfigurationManager._write_configuration(
        config_file,
        Configuration(db_dir=os.path.join(tmp_path, 'blabla'),
                      download_dir=os.path.join(tmp_path, 'blabla'),
                      user_agent_email='user@email.com',
                      parquet_dir=os.path.join(tmp_path, 'parquet'),
                      auto_update=False))  # prevent calling update

    configuration = ConfigurationManager.read_config_file()
    assert configuration is not None
    assert configuration.db_dir.endswith('blabla')
    # check some default values
    assert configuration.auto_update is False
    assert configuration.keep_zip_files is False


def test_no_config_file_in_home_no_update(tmp_path, monkeypatch):
    # test if a file is created at the home directory
    with patch('os.path.expanduser') as mock_expanduser:
        mock_expanduser.return_value = str(tmp_path)
        # configure command line input for _handle_first_start method
        monkeypatch.setattr('sys.stdin', StringIO('n\n'))

        with pytest.raises(ValueError):
            ConfigurationManager.read_config_file()

            # check if file is present
            home_cfg_file_path = os.path.join(os.path.expanduser('~'), DEFAULT_CONFIG_FILE)
            assert os.path.isfile(home_cfg_file_path)


def test_no_config_file_in_home_with_update(tmp_path, monkeypatch):
    # test if a file is created at the home directory
    with patch('os.path.expanduser') as mock_expanduser, \
            patch(
                'secfsdstools.a_config.configmgt.ConfigurationManager._do_initial_update') as mock_update:
        mock_update.return_value = None
        mock_expanduser.return_value = str(tmp_path)
        # configure command line input for _handle_first_start method
        monkeypatch.setattr('sys.stdin', StringIO('y\n'))

        ConfigurationManager.read_config_file()

        mock_update.assert_called_once()

        # check if file is present
        home_cfg_file_path = os.path.join(os.path.expanduser('~'), DEFAULT_CONFIG_FILE)
        assert os.path.isfile(home_cfg_file_path)


def test_config_file_in_home(tmp_path):
    config_file = str(tmp_path / DEFAULT_CONFIG_FILE)

    ConfigurationManager._write_configuration(
        config_file,
        Configuration(db_dir=os.path.join(tmp_path, 'bloblo'),
                      download_dir=os.path.join(tmp_path, 'bloblo'),
                      user_agent_email='user@email.com',
                      parquet_dir=os.path.join(tmp_path, 'parquet'),
                      auto_update=False))  # prevent calling update

    with patch('os.path.expanduser') as mock_expanduser:
        mock_expanduser.return_value = str(tmp_path)

        configuration = ConfigurationManager.read_config_file()
        assert configuration is not None
        assert configuration.db_dir.endswith('bloblo')
        # check some default values
        assert configuration.auto_update is False
        assert configuration.keep_zip_files is False


def test_config_file_in_home_call_to_updated(tmp_path):
    # check if update-method is actually called
    config_file = str(tmp_path / DEFAULT_CONFIG_FILE)

    ConfigurationManager._write_configuration(
        config_file,
        Configuration(db_dir=os.path.join(tmp_path, 'bloblo'),
                      download_dir=os.path.join(tmp_path, 'bloblo'),
                      user_agent_email='user@email.com',
                      parquet_dir=os.path.join(tmp_path, 'parquet')))

    with patch('os.path.expanduser') as mock_expanduser:
        mock_expanduser.return_value = str(tmp_path)

        configuration = ConfigurationManager.read_config_file()
        assert configuration is not None
        assert configuration.db_dir.endswith('bloblo')
        # check some default values
        assert configuration.auto_update is True
        assert configuration.keep_zip_files is False


def test_check_basic_configuration(tmp_path):
    invalid_email_config = Configuration(db_dir=str(tmp_path),
                                         download_dir=str(tmp_path),
                                         user_agent_email='email.com',
                                         parquet_dir=os.path.join(tmp_path, 'parquet'))

    results = ConfigurationManager.check_basic_configuration(invalid_email_config)
    assert len(results) == 1
    assert 'UserAgentEmail' in results[0]


def my_update_hook(config: Configuration):
    pass


def my_update_hook_no_param_type(config):
    pass


def my_update_hook_wrong_param_type(config: str):
    pass


def test__validate_post_update_hook():
    assert len(ConfigurationManager._validate_post_update_hook(
        "tests.a_config.test_configmgt.my_update_hook")) == 0
    assert len(ConfigurationManager._validate_post_update_hook(
        "tests.a_config.test_configmgt.my_update_hook_no_param_type")) == 0
    assert len(ConfigurationManager._validate_post_update_hook(
        "tests.a_config.test_configmgt.my_update_hook_wrong_param_type")) == 2

    # wrong function name
    assert len(ConfigurationManager._validate_post_update_hook(
        "tests.a_config.test_configmgt.wrongfunction")) == 2

    # wrong module name
    assert len(ConfigurationManager._validate_post_update_hook(
        "tests.unknown.test_configmgt.wrongfunction")) == 2


def my_update_processfunc(config: Configuration) -> List[AbstractProcess]:
    pass


def my_update_processfunc_no_param_type(config) -> List[AbstractProcess]:
    pass


def my_update_processfunc_wrong_param_type(config: str) -> List[AbstractProcess]:
    pass


def my_update_processfunc_no_return_type(config: Configuration):
    pass


def my_update_processfunc_wrong_return_type(config: Configuration) -> List[str]:
    pass


def test__validate_post_update_processes():
    assert len(ConfigurationManager._validate_post_update_processes(
        "tests.a_config.test_configmgt.my_update_processfunc")) == 0
    assert len(ConfigurationManager._validate_post_update_processes(
        "tests.a_config.test_configmgt.my_update_processfunc_no_param_type")) == 0
    assert len(ConfigurationManager._validate_post_update_processes(
        "tests.a_config.test_configmgt.my_update_processfunc_wrong_param_type")) == 2
    assert len(ConfigurationManager._validate_post_update_processes(
        "tests.a_config.test_configmgt.my_update_processfunc_no_return_type")) == 0
    assert len(ConfigurationManager._validate_post_update_processes(
        "tests.a_config.test_configmgt.my_update_processfunc_wrong_return_type")) == 2

    # wrong function name
    assert len(ConfigurationManager._validate_post_update_processes(
        "tests.a_config.test_configmgt.wrongfunction")) == 2

    # wrong module name
    assert len(ConfigurationManager._validate_post_update_processes(
        "tests.unknown.test_configmgt.wrongfunction")) == 2


def test_check_rapid_configuration(tmp_path):
    # either to mock the db calls for get_key/set_key or providing a db db
    DbCreator(db_dir=str(tmp_path)).create_db()

    invalid_rapid_plan = Configuration(db_dir=str(tmp_path),
                                       download_dir=str(tmp_path),
                                       user_agent_email='abc@xy.org',
                                       rapid_api_plan='bl',
                                       parquet_dir=os.path.join(tmp_path, 'parquet'))

    results = ConfigurationManager.check_rapid_configuration(invalid_rapid_plan)
    assert len(results) == 1
    assert 'RapidApiPlan' in results[0]

    valid_rapid_plan = Configuration(db_dir=str(tmp_path),
                                     download_dir=str(tmp_path),
                                     user_agent_email='abc@xy.org',
                                     rapid_api_plan='basic',
                                     parquet_dir=os.path.join(tmp_path, 'parquet'))

    results = ConfigurationManager.check_rapid_configuration(valid_rapid_plan)
    assert len(results) == 0

    valid_rapid_plan = Configuration(db_dir=str(tmp_path),
                                     download_dir=str(tmp_path),
                                     user_agent_email='abc@xy.org',
                                     rapid_api_plan='premium',
                                     parquet_dir=os.path.join(tmp_path, 'parquet'))
    results = ConfigurationManager.check_rapid_configuration(valid_rapid_plan)
    assert len(results) == 0

    valid_rapid_plan = Configuration(db_dir=str(tmp_path),
                                     download_dir=str(tmp_path),
                                     user_agent_email='abc@xy.org',
                                     rapid_api_plan=None,
                                     parquet_dir=os.path.join(tmp_path, 'parquet'))
    results = ConfigurationManager.check_rapid_configuration(valid_rapid_plan)
    assert len(results) == 0

    rapid_api_key = os.environ.get('RAPID_API_KEY')
    valid_api_key = Configuration(db_dir=str(tmp_path),
                                  download_dir=str(tmp_path),
                                  user_agent_email='abc@xy.com',
                                  rapid_api_key=rapid_api_key,
                                  parquet_dir=os.path.join(tmp_path, 'parquet'))
    results = ConfigurationManager.check_rapid_configuration(valid_api_key)
    print(results)
    assert len(results) == 0

    invalid_api_key = Configuration(db_dir=str(tmp_path),
                                    download_dir=str(tmp_path),
                                    user_agent_email='abc@xy.com',
                                    rapid_api_key="abc",
                                    parquet_dir=os.path.join(tmp_path, 'parquet'))
    results = ConfigurationManager.check_rapid_configuration(invalid_api_key)
    assert len(results) == 1
    assert 'RapidApiKey' in results[0]
