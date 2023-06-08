import os
from unittest.mock import patch

import pytest

from secfsdstools.c_index.indexdataaccess import IndexReport
from secfsdstools.e_collector.multireportcollecting import MultiReportCollector

APPLE_ADSH_10Q_2010_Q1 = '0001193125-10-012085'
APPLE_ADSH_10Q_2010_Q2 = '0001193125-10-088957'

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_PARQUET_Q1 = f'{CURRENT_DIR}/testdataparquet/quarter/2010q1.zip'
PATH_TO_PARQUET_Q2 = f'{CURRENT_DIR}/testdataparquet/quarter/2010q2.zip'


@pytest.fixture
def multireportcollector():
    report1 = IndexReport(adsh=APPLE_ADSH_10Q_2010_Q1, cik=320193, name='APPLE INC',
                          form='10-Q', filed=20100125, period=20091231, originFile='2010q1.zip',
                          originFileType='quarter', fullPath=PATH_TO_PARQUET_Q1, url='')
    report2 = IndexReport(adsh=APPLE_ADSH_10Q_2010_Q2, cik=320193, name='APPLE INC',
                          form='10-Q', filed=20100421, period=20100331, originFile='2010q2.zip',
                          originFileType='quarter', fullPath=PATH_TO_PARQUET_Q2, url='')

    reports = [report1, report2]

    collector = MultiReportCollector.get_reports_by_indexreports(index_reports=reports)
    collector.collect()
    return collector


def test_cm_get_report_by_adshs():
    indexreports = [
        IndexReport(cik=320193, name="", form="", filed=0, period=0, originFile="",
                    originFileType="", url="",
                    adsh=APPLE_ADSH_10Q_2010_Q1,
                    fullPath=PATH_TO_PARQUET_Q1),
        IndexReport(cik=320193, name="", form="", filed=0, period=0, originFile="",
                    originFileType="", url="",
                    adsh=APPLE_ADSH_10Q_2010_Q2,
                    fullPath=PATH_TO_PARQUET_Q2),
    ]

    with patch(
            "secfsdstools.c_index.indexdataaccess.ParquetDBIndexingAccessor.read_index_reports_for_adshs",
            return_value=indexreports):
        collector = MultiReportCollector.get_reports_by_adshs(
            [APPLE_ADSH_10Q_2010_Q1, APPLE_ADSH_10Q_2010_Q2])

        assert collector.collect().num_df.shape == (321, 9)


def test_read_raw_data(multireportcollector):
    assert multireportcollector.databag.sub_df.shape == (2, 36)
    assert multireportcollector.databag.num_df.shape == (321, 9)
    assert multireportcollector.databag.pre_df.shape == (211, 10)


def test_statistics(multireportcollector):
    stats = multireportcollector.statistics()

    assert stats.number_of_reports == 2
    assert stats.num_entries == 321
    assert stats.pre_entries == 211
    assert stats.reports_per_form['10-Q'] == 2
    assert stats.reports_per_period_date[20091231] == 1
    assert stats.reports_per_period_date[20100331] == 1
