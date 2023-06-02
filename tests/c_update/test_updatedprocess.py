import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
from secfsdstools.c_update.updateprocess import Updater

current_dir, _ = os.path.split(__file__)


@pytest.fixture
def updater(tmp_path: Path) -> Updater:
    db_dir = str(tmp_path / 'db')

    DbCreator(db_dir=db_dir).create_db()

    return Updater(
        db_dir=db_dir,
        dld_dir=str(tmp_path / 'dld'),
        daily_dld_dir=str(tmp_path / 'dlddaily'),
        parquet_dir=str(tmp_path / 'parquet'),
        user_agent="me@here.com",
        keep_zip_files=True,
        rapid_api_plan=None,
        rapid_api_key=None,
    )


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


def test_do_download_no_rapid_api(updater):
    with patch('secfsdstools.c_download.secdownloading.SecZipDownloader.download') \
            as sec_download, \
            patch('secfsdstools.c_download.rapiddownloading.RapidZipDownloader.download') \
                    as rapid_download:
        updater._do_download()

        # Überprüfen, ob die download-Methode von SecZipDownloader aufgerufen wurde
        sec_download.assert_called_once()
        rapid_download.assert_not_called()


def test_do_download_with_rapid_api(updater):
    updater.rapid_api_key = "akey"
    updater.rapid_api_plan = "basic"

    with patch('secfsdstools.c_download.secdownloading.SecZipDownloader.download') \
            as sec_download, \
            patch('secfsdstools.c_download.rapiddownloading.RapidZipDownloader.download') \
                    as rapid_download:
        updater._do_download()

        # Überprüfen, ob die download-Methode von SecZipDownloader aufgerufen wurde
        sec_download.assert_called_once()
        rapid_download.assert_called_once()


def test_do_transform_none_existing_folder(updater):
    updater.dld_dir = "./bla"
    updater._do_transform()


def test_do_transform_and_index(updater):
    updater.dld_dir = f'{current_dir}/testdata_zip'
    updater._do_transform()

    transformed = get_directories_in_directory(os.path.join(updater.parquet_dir, 'quarter'))
    assert len(transformed) == 2

    # test indexing
    updater._do_index()

    indexer = ParquetDBIndexingAccessor(db_dir=updater.db_dir)
    indexed_files_df = indexer.read_all_indexfileprocessing_df()
    assert len(indexed_files_df) == 2

    reports_df = indexer.read_all_indexreports_df()
    assert len(reports_df) == 1017


def test_integration_test(updater):
    updater.dld_dir = f'{current_dir}/testdata_zip'

    # check that there is no state key at the beginning
    last_check = updater.db_state_accesor.get_key(Updater.LAST_UPDATE_CHECK_KEY)
    assert last_check is None

    start_time = time.time()
    time.sleep(1) # make sure some time has past, before calling update
    with patch('secfsdstools.c_download.secdownloading.SecZipDownloader.download') \
            as sec_download, \
            patch('secfsdstools.c_download.rapiddownloading.RapidZipDownloader.download') \
                    as rapid_download:

        # updates LAST_UPDATE_CHECK_KEY
        updater.update()

        # Überprüfen, ob die download-Methode von SecZipDownloader aufgerufen wurde
        sec_download.assert_called_once()
        rapid_download.assert_not_called()

        # check that both zip files had been processed
        indexer = ParquetDBIndexingAccessor(db_dir=updater.db_dir)
        indexed_files_df = indexer.read_all_indexfileprocessing_df()
        assert len(indexed_files_df) == 2

        reports_df = indexer.read_all_indexreports_df()
        assert len(reports_df) == 1017

        # check that
        last_check = updater.db_state_accesor.get_key(Updater.LAST_UPDATE_CHECK_KEY)
        assert start_time < float(last_check)

