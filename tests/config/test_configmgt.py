import os

from secfsdstools.config.configmgt import ConfigurationManager, Configuration

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
NOT_EXISTING_CFG = CURRENT_DIR + '/config.cfg'


def test_creation_of_configuration_file():
    # test if file exists -> remove
    if os.path.exists(NOT_EXISTING_CFG):
        os.remove(NOT_EXISTING_CFG)

    cfgmgt = ConfigurationManager(NOT_EXISTING_CFG)

    # test if file exists
    assert os.path.exists(NOT_EXISTING_CFG)
    configuration: Configuration = cfgmgt.get_configuration()

    assert configuration.download_dir == './dld'
    assert configuration.create_index is True


    # reread the file
    cfgmgt2 = ConfigurationManager(NOT_EXISTING_CFG)
    configuration2 = cfgmgt2.get_configuration()
    assert configuration2.download_dir == './dld'
    assert configuration2.create_index is True

    if os.path.exists(NOT_EXISTING_CFG):
        os.remove(NOT_EXISTING_CFG)


def test_read_existing_configuration_file():
    file1 = CURRENT_DIR + "/resources/cfg1.cfg"

    cfgmgt1 = ConfigurationManager(file1)
    configuration1 = cfgmgt1.get_configuration()
    assert configuration1.download_dir == './cfg1'
    assert configuration1.create_index is True

    file2 = CURRENT_DIR + "/resources/cfg2.cfg"
    cfgmgt2 = ConfigurationManager(file2)
    configuration2 = cfgmgt2.get_configuration()
    assert configuration2.download_dir == './cfg2'
    assert configuration2.create_index is False