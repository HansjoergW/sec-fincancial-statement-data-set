import os
from unittest.mock import patch

import pytest

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexFileProcessingState
from secfsdstools.e_collector.zipcollecting import ZipCollector

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = f'{CURRENT_DIR}/testdataparquet/quarter/2010q1.zip'


@pytest.fixture
def zipcollector():
    zipcollector = ZipCollector(datapath=PATH_TO_ZIP)
    zipcollector.collect()
    return zipcollector


def test_cm_get_zip_by_name():
    instance = IndexFileProcessingState(fileName="", status="", entries=0, processTime="",
                                        fullPath=PATH_TO_ZIP)

    with patch(
            "secfsdstools.c_index.indexdataaccess.ParquetDBIndexingAccessor.read_index_file_for_filename",
            return_value=instance):
        zipcollector = ZipCollector.get_zip_by_name(name="2010q1.zip",
                                                    configuration=Configuration(db_dir="",
                                                                                download_dir="",
                                                                                user_agent_email="",
                                                                                parquet_dir=""))
        assert zipcollector.collect().num_df.shape == (151692, 9)


def test_read_raw_data(zipcollector):
    assert zipcollector.collect().num_df.shape == (151692, 9)
    assert zipcollector.collect().pre_df.shape == (88378, 10)
    assert zipcollector.collect().sub_df.shape == (495, 36)


def test_statistics(zipcollector):
    stats = zipcollector.statistics()

    assert stats.num_entries == 151692
    assert stats.pre_entries == 88378
    assert stats.number_of_reports == 495
    assert stats.reports_per_form['10-Q'] == 80
    assert stats.reports_per_period_date[20091231] == 434
