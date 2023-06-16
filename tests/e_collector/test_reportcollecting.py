import os
from unittest.mock import patch

import pytest

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexReport
from secfsdstools.e_collector.reportcollecting import SingleReportCollector

APPLE_ADSH_10Q_2010_Q1 = '0001193125-10-012085'
CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = f'{CURRENT_DIR}/testdataparquet/quarter/2010q1.zip'


@pytest.fixture
def reportcollector():
    report = IndexReport(adsh=APPLE_ADSH_10Q_2010_Q1, cik=320193, name='APPLE INC',
                         form='10-Q', filed=20100125, period=20091231, originFile='2010q1.zip',
                         originFileType='quarter', fullPath=PATH_TO_ZIP, url='')

    return SingleReportCollector(report=report)


def test_cm_get_report_by_adsh():
    instance = IndexReport(cik=320193, name="", form="", filed=0, period=0, originFile="",
                           originFileType="", url="",
                           adsh=APPLE_ADSH_10Q_2010_Q1,
                           fullPath=PATH_TO_ZIP)

    with patch(
            "secfsdstools.c_index.indexdataaccess.ParquetDBIndexingAccessor.read_index_report_for_adsh",
            return_value=instance):
        reportreader = SingleReportCollector.get_report_by_adsh(
            adsh=APPLE_ADSH_10Q_2010_Q1,
            configuration=Configuration(db_dir="",
                                        download_dir="",
                                        user_agent_email="",
                                        parquet_dir=""))
        assert reportreader.collect().num_df.shape == (145, 9)


def test_read_raw_data(reportcollector):
    bag = reportcollector.collect()
    assert bag.num_df.shape == (145, 9)
    assert bag.pre_df.shape == (100, 10)
