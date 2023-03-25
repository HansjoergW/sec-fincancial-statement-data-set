import os
from unittest.mock import patch

import pytest

from secfsdstools.a_config.configmgt import Configuration
from secfsdstools.d_index.indexdataaccess import IndexReport
from secfsdstools.e_read.reportreading import ReportReader

APPLE_ADSH_10Q_2010_Q1 = '0001193125-10-012085'
CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = CURRENT_DIR + '/testdata/2010q1.zip'


@pytest.fixture
def reportreader():
    report = IndexReport(adsh=APPLE_ADSH_10Q_2010_Q1, cik=320193, name='APPLE INC',
                         form='10-Q', filed=20100125, period=20091231, originFile='2010q1.zip',
                         originFileType='quarter', fullPath=PATH_TO_ZIP, url='')

    reportreader = ReportReader(report=report)
    reportreader._read_raw_data()
    return reportreader


def test_read_raw_data(reportreader):
    assert reportreader.num_df.shape == (145, 9)
    assert reportreader.pre_df.shape == (100, 10)


def test_financial_statements(reportreader):
    # read only for the actual period
    fin_stmts_df = reportreader.financial_statements_for_period()

    assert fin_stmts_df.shape == (74, 12)

    # read for the actual period and the previous period
    fin_stmts_df = reportreader.financial_statements_for_period_and_previous_period()

    assert fin_stmts_df.shape == (74, 13)

    # read only for the actual period
    fin_stmts_df = reportreader.financial_statements_for_period(tags=['Assets'])
    assert fin_stmts_df.shape == (1, 12)


def test_submission_data(reportreader):
    data = reportreader.submission_data()

    assert data['adsh'] == APPLE_ADSH_10Q_2010_Q1
    assert data['cik'] == 320193


def test_statistics(reportreader):
    stats = reportreader.statistics()

    assert stats.num_entries == 145
    assert stats.pre_entries == 100
    assert len(set(stats.list_of_statements) - {'BS', 'CF', 'CP', 'IS'}) == 0


def test_cm_get_report_by_adsh():
    instance = IndexReport(cik=320193, name="", form="", filed=0, period=0, originFile="", originFileType="", url="",
                           adsh=APPLE_ADSH_10Q_2010_Q1,
                           fullPath=PATH_TO_ZIP)

    with patch("secfsdstools.d_index.indexdataaccess.DBIndexingAccessor.read_index_report_for_adsh",
               return_value=instance):
        reportreader = ReportReader.get_report_by_adsh(
            adsh=APPLE_ADSH_10Q_2010_Q1,
            configuration=Configuration(db_dir="",
                                        download_dir="",
                                        user_agent_email=""))
        reportreader._read_raw_data()
        assert reportreader.num_df.shape == (145, 9)
