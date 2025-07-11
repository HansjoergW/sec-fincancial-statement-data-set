import os
from unittest.mock import patch

import pytest

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexReport
from secfsdstools.e_collector.reportcollecting import SingleReportCollector

APPLE_ADSH_10Q_2010_Q1 = "0001193125-10-012085"
CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = f"{CURRENT_DIR}/../_testdata/parquet_new/quarter/2010q1.zip"
PATH_TO_DAILY_ZIP = f"{CURRENT_DIR}/../_testdata/parquet_new/daily/20250701.zip"


@pytest.fixture
def reportcollector():
    report = IndexReport(
        adsh=APPLE_ADSH_10Q_2010_Q1,
        cik=320193,
        name="APPLE INC",
        form="10-Q",
        filed=20100125,
        period=20091231,
        originFile="2010q1.zip",
        originFileType="quarter",
        fullPath=PATH_TO_ZIP,
        url="",
    )

    return SingleReportCollector(report=report)


@pytest.fixture
def dailyreportcollector():
    report = IndexReport(
        adsh="0001554795-25-000172",
        cik=1394108,
        name="SUIC WORLDWIDE HOLDINGS LTD.",
        form="10-Q",
        filed=20250701,
        period=20250331,
        originFile="20250701.zip",
        originFileType="daily",
        fullPath=PATH_TO_DAILY_ZIP,
        url="",
    )

    return SingleReportCollector(report=report)


def test_cm_get_report_by_adsh():
    instance = IndexReport(
        cik=320193,
        name="",
        form="",
        filed=0,
        period=0,
        originFile="",
        originFileType="",
        url="",
        adsh=APPLE_ADSH_10Q_2010_Q1,
        fullPath=PATH_TO_ZIP,
    )

    with patch(
        "secfsdstools.c_index.indexdataaccess.ParquetDBIndexingAccessor.read_index_report_for_adsh",
        return_value=instance,
    ):
        reportreader = SingleReportCollector.get_report_by_adsh(
            adsh=APPLE_ADSH_10Q_2010_Q1,
            configuration=Configuration(db_dir="", download_dir="", user_agent_email="", parquet_dir=""),
        )
        assert reportreader.collect().num_df.shape == (144, 10)


def test_cm_get_daily_report_by_adsh():
    instance = IndexReport(
        cik=1394108,
        name="",
        form="",
        filed=0,
        period=0,
        originFile="",
        originFileType="",
        url="",
        adsh="0001554795-25-000172",
        fullPath=PATH_TO_DAILY_ZIP,
    )

    with patch(
        "secfsdstools.c_index.indexdataaccess.ParquetDBIndexingAccessor.read_index_report_for_adsh",
        return_value=instance,
    ):
        reportreader = SingleReportCollector.get_report_by_adsh(
            adsh="0001554795-25-000172",
            configuration=Configuration(db_dir="", download_dir="", user_agent_email="", parquet_dir=""),
        )
        assert reportreader.collect().num_df.shape == (128, 10)


def test_read_raw_data(reportcollector):
    bag = reportcollector.collect()
    assert bag.num_df.shape == (144, 10)
    assert bag.pre_df.shape == (74, 10)


def test_read_raw_daily_data(dailyreportcollector):
    bag = dailyreportcollector.collect()
    assert bag.num_df.shape == (128, 10)
    assert bag.pre_df.shape == (69, 10)
