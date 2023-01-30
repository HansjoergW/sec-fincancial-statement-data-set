import os
from unittest.mock import patch

import pytest

from secfsdstools.a_config.configmgt import ConfigurationManager, Configuration, \
    SECFSDSTOOLS_ENV_VAR_NAME, DEFAULT_CONFIG_FILE

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
NOT_EXISTING_CFG = CURRENT_DIR + '/config.cfg'


def test_environment_variable_no_file(tmp_path):
    # check if file is created if the env variable is set
    config_file = str(tmp_path) + '/test.cfg'
    with patch.dict(os.environ, {SECFSDSTOOLS_ENV_VAR_NAME: config_file}, clear=True):
        with pytest.raises(ValueError):
            ConfigurationManager.read_config_file()

            # check if file is present
            env_config_file = os.getenv(SECFSDSTOOLS_ENV_VAR_NAME)
            assert os.path.isfile(env_config_file)


def test_environment_variable_with_file(tmp_path):
    # check if file is read if the env variable is set

    # create the configuration at the expected location
    config_file = str(tmp_path) + '/test.cfg'
    ConfigurationManager._write_configuration(config_file,
                                              Configuration(db_dir=os.path.join(tmp_path, 'blublu'),
                                                            download_dir=os.path.join(tmp_path, 'blublu'),
                                                            user_agent_email='user@email.com'))

    with patch.dict(os.environ, {SECFSDSTOOLS_ENV_VAR_NAME: config_file}, clear=True):
        configuration = ConfigurationManager.read_config_file()
        assert configuration is not None
        assert configuration.db_dir.endswith('blublu')


# Tests
def test_config_file_in_cwd(tmp_path, monkeypatch: pytest.MonkeyPatch):
    # tests if the config file is read from the current directory if present

    # patch the current working dir to a temp path
    monkeypatch.chdir(tmp_path)
    config_file = str(tmp_path / DEFAULT_CONFIG_FILE)

    ConfigurationManager._write_configuration(config_file,
                                              Configuration(db_dir=os.path.join(tmp_path, 'blabla'),
                                                            download_dir=os.path.join(tmp_path, 'blabla'),
                                                            user_agent_email='user@email.com'))

    configuration = ConfigurationManager.read_config_file()
    assert configuration is not None
    assert configuration.db_dir.endswith('blabla')


def test_no_config_file_in_home(tmp_path):
    # test if a file is created at the home directory
    with patch('os.path.expanduser') as mock_expanduser:
        mock_expanduser.return_value = str(tmp_path)

        with pytest.raises(ValueError):
            ConfigurationManager.read_config_file()

            # check if file is present
            home_cfg_file_path = os.path.join(os.path.expanduser('~'), DEFAULT_CONFIG_FILE)
            assert os.path.isfile(home_cfg_file_path)


def test_config_file_in_home(tmp_path):
    config_file = str(tmp_path / DEFAULT_CONFIG_FILE)

    ConfigurationManager._write_configuration(config_file,
                                              Configuration(db_dir=os.path.join(tmp_path, 'bloblo'),
                                                            download_dir=os.path.join(tmp_path, 'bloblo'),
                                                            user_agent_email='user@email.com'))
    with patch('os.path.expanduser') as mock_expanduser:
        mock_expanduser.return_value = str(tmp_path)

        configuration = ConfigurationManager.read_config_file()
        assert configuration is not None
        assert configuration.db_dir.endswith('bloblo')


def test_check_basic_configuration(tmp_path):
    invalid_email_config = Configuration(db_dir=str(tmp_path),
                                         download_dir=str(tmp_path),
                                         user_agent_email='email.com')

    results = ConfigurationManager.check_basic_configuration(invalid_email_config)
    assert len(results) == 1
    assert 'UserAgentEmail' in results[0]


def test_check_rapid_configuration(tmp_path):
    invalid_rapid_plan = Configuration(db_dir=str(tmp_path),
                                       download_dir=str(tmp_path),
                                       user_agent_email='abc@xy.org',
                                       rapid_api_plan='bl')

    results = ConfigurationManager.check_rapid_configuration(invalid_rapid_plan)
    assert len(results) == 1
    assert 'RapidApiPlan' in results[0]

    valid_rapid_plan = Configuration(db_dir=str(tmp_path),
                                     download_dir=str(tmp_path),
                                     user_agent_email='abc@xy.org',
                                     rapid_api_plan='basic')

    results = ConfigurationManager.check_rapid_configuration(valid_rapid_plan)
    assert len(results) == 0

    valid_rapid_plan = Configuration(db_dir=str(tmp_path),
                                     download_dir=str(tmp_path),
                                     user_agent_email='abc@xy.org',
                                     rapid_api_plan='premium')
    results = ConfigurationManager.check_rapid_configuration(valid_rapid_plan)
    assert len(results) == 0

    valid_rapid_plan = Configuration(db_dir=str(tmp_path),
                                     download_dir=str(tmp_path),
                                     user_agent_email='abc@xy.org',
                                     rapid_api_plan=None)
    results = ConfigurationManager.check_rapid_configuration(valid_rapid_plan)
    assert len(results) == 0

    rapid_api_key = os.environ.get('RAPID_API_KEY')
    valid_api_key = Configuration(db_dir=str(tmp_path),
                                  download_dir=str(tmp_path),
                                  user_agent_email='abc@xy.com',
                                  rapid_api_key=rapid_api_key)
    results = ConfigurationManager.check_rapid_configuration(valid_api_key)
    assert len(results) == 0

    invalid_api_key = Configuration(db_dir=str(tmp_path),
                                    download_dir=str(tmp_path),
                                    user_agent_email='abc@xy.com',
                                    rapid_api_key="abc")
    results = ConfigurationManager.check_rapid_configuration(invalid_api_key)
    assert len(results) == 1
    assert 'RapidApiKey' in results[0]
