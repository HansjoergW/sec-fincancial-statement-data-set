import time
from pathlib import Path
from unittest.mock import patch

import pytest

from secfsdstools.c_update.updateprocess import Updater


@pytest.fixture
def updater(tmp_path: Path) -> Updater:
    db_dir = str(tmp_path / 'db')

    # DbCreator(db_dir=db_dir).create_db()

    return Updater(
        db_dir=db_dir,
        dld_dir=str(tmp_path / 'dld'),
        daily_dld_dir=str(tmp_path / 'dlddaily'),
        parquet_dir=str(tmp_path / 'parquet'),
        user_agent="me@here.com",
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


wie reagiert daily_transformer, falls kein daily download dir vorhanden ist? müsste auch funktionieren

dito indexer
