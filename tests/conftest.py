import os

import pytest

from secfsdstools.a_config.configmodel import Configuration


CURRENT_DIR, _ = os.path.split(__file__)


@pytest.fixture
def basicconf() -> Configuration:
    return Configuration(
        download_dir=f"{CURRENT_DIR}/_testdata/zip",
        db_dir=CURRENT_DIR,
        parquet_dir=f"{CURRENT_DIR}/_testdata/parquet_new",
        user_agent_email="me@home.com",
        rapid_api_key=None,
        rapid_api_plan="basic",
        daily_download_dir="",
        auto_update=False,
        keep_zip_files=False)