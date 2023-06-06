import os

import pytest

from secfsdstools.c_index.indexdataaccess import IndexReport
from secfsdstools.e_read.reportreading import ReportReader

APPLE_ADSH_10Q_2010_Q1 = '0001193125-10-012085'
CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = f'{CURRENT_DIR}/testdataparquet/quarter/2010q1.zip'


@pytest.fixture
def reportreader():
    report = IndexReport(adsh=APPLE_ADSH_10Q_2010_Q1, cik=320193, name='APPLE INC',
                         form='10-Q', filed=20100125, period=20091231, originFile='2010q1.zip',
                         originFileType='quarter', fullPath=PATH_TO_ZIP, url='')

    reportreader = ReportReader(report=report)
    reportreader._read_raw_data()
    return reportreader


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
