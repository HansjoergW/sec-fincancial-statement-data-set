import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
from secfsdstools.c_update.updateprocess import Updater

current_dir, _ = os.path.split(__file__)


@pytest.fixture
def updater(tmp_path: Path) -> Updater:
    db_dir = str(tmp_path / 'db')

    DbCreator(db_dir=db_dir).create_db()

    config: Configuration = Configuration(
        db_dir=db_dir,
        download_dir=str(tmp_path / 'dld'),
        daily_download_dir=str(tmp_path / 'dlddaily'),
        parquet_dir=str(tmp_path / 'parquet'),
        user_agent_email="me@here.com",
        keep_zip_files=True,
        auto_update=True,
        rapid_api_plan=None,
        rapid_api_key=None,
    )

    return Updater(config=config)


def test_check_for_update(updater):
    # check if no UPDATE_CHECK_KEY is present
    with patch("secfsdstools.a_utils.dbutils.DBStateAcessor.get_key", return_value=None):
        assert updater._check_for_update() is True

    # check if last update was 5 hours ago
    with patch("secfsdstools.a_utils.dbutils.DBStateAcessor.get_key",
               return_value=time.time() - (3600 * 2)):
        assert updater._check_for_update() is False

    # check if  last update was 25 hours ago
    with patch("secfsdstools.a_utils.dbutils.DBStateAcessor.get_key",
               return_value=time.time() - (3600 * 25)):
        assert updater._check_for_update() is True


def test_update_no_rapid_api(updater):
    with patch('secfsdstools.c_download.secdownloading_process.SecDownloadingProcess.process') \
            as sec_download, \
            patch(
                'secfsdstools.c_download.rapiddownloading_process.RapidDownloadingProcess.process') \
                    as rapid_download, \
            patch(
                'secfsdstools.c_transform.toparquettransforming_process.ToParquetTransformerProcess.process') \
                    as transformer, \
            patch(
                'secfsdstools.c_index.indexing_process.ReportParquetIndexerProcess.process') \
                    as indexer:
        updater._update()

        # Überprüfen, ob die download-Methode von SecZipDownloader aufgerufen wurde
        sec_download.assert_called_once()
        rapid_download.assert_not_called()
        transformer.assert_called_once()
        indexer.assert_called_once()


def test_update_with_rapid_api(updater):
    updater.rapid_api_key = "akey"
    updater.rapid_api_plan = "basic"

    with patch('secfsdstools.c_download.secdownloading_process.SecDownloadingProcess.process') \
            as sec_download, \
            patch(
                'secfsdstools.c_download.rapiddownloading_process.RapidDownloadingProcess.process') \
                    as rapid_download, \
            patch(
                'secfsdstools.c_transform.toparquettransforming_process.ToParquetTransformerProcess.process') \
                    as transformer, \
            patch(
                'secfsdstools.c_index.indexing_process.ReportParquetIndexerProcess.process') \
                    as indexer:
        updater._update()

        # Überprüfen, ob die download-Methode von SecZipDownloader aufgerufen wurde
        sec_download.assert_called_once()
        rapid_download.assert_called_once()
        assert transformer.call_count == 2
        assert indexer.call_count == 2


def test_integration_test(updater):
    updater.dld_dir = f'{current_dir}/../_testdata/zip'

    # check that there is no state key at the beginning
    last_check = updater.db_state_accesor.get_key(Updater.LAST_UPDATE_CHECK_KEY)
    assert last_check is None

    start_time = time.time()
    time.sleep(1)  # make sure some time has past, before calling update
    with patch('secfsdstools.c_download.secdownloading_process.SecDownloadingProcess.process') \
            as sec_download, \
            patch(
                'secfsdstools.c_download.rapiddownloading_process.RapidDownloadingProcess.process') \
                    as rapid_download:
        # updates LAST_UPDATE_CHECK_KEY
        updater.update()

        # Überprüfen, ob die download-Methode von SecZipDownloader aufgerufen wurde
        sec_download.assert_called_once()
        rapid_download.assert_not_called()

        # check that both zip files had been processed
        indexer = ParquetDBIndexingAccessor(db_dir=updater.db_dir)
        indexed_files_df = indexer.read_all_indexfileprocessing_df()
        assert len(indexed_files_df) == 3

        reports_df = indexer.read_all_indexreports_df()
        assert len(reports_df) == 1456

        # check that
        last_check = updater.db_state_accesor.get_key(Updater.LAST_UPDATE_CHECK_KEY)
        assert start_time < float(last_check)


def update_hook(config: Configuration):
    print("update_hook_called")


def update_processes_hook(config: Configuration):
    print("update_processes_hook")
    return []


def test_update_hooks(tmp_path):
    db_dir = str(tmp_path / 'db')

    DbCreator(db_dir=db_dir).create_db()

    config: Configuration = Configuration(
        db_dir=db_dir,
        download_dir=str(tmp_path / 'dld'),
        daily_download_dir=str(tmp_path / 'dlddaily'),
        parquet_dir=str(tmp_path / 'parquet'),
        user_agent_email="me@here.com",
        keep_zip_files=True,
        auto_update=True,
        rapid_api_plan=None,
        rapid_api_key=None,
        post_update_hook="tests.c_update.test_updatedprocess.update_hook",
        post_update_processes="tests.c_update.test_updatedprocess.update_processes_hook"
    )

    updater = Updater(config=config)
    with patch('secfsdstools.c_download.secdownloading_process.SecDownloadingProcess.process') \
            as sec_download, \
            patch(
                'secfsdstools.c_download.rapiddownloading_process.RapidDownloadingProcess.process') \
                    as rapid_download, \
            patch('tests.c_update.test_updatedprocess.update_hook') as update_hook_patch, \
            patch('tests.c_update.test_updatedprocess.update_processes_hook', return_value=[]) as update_processes_hook_patch:
        updater.update()

        update_hook_patch.assert_called_once()
        update_processes_hook_patch.assert_called_once()
