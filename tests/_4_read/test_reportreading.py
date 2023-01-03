import os

import pytest

from secfsdstools._3_index.indexdataaccess import IndexReport
from secfsdstools._4_read.reportreading import ReportReader

APPLE_ADSH_10Q_2010_Q1 = '0001193125-10-012085'
CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = CURRENT_DIR + '/testdata/'


@pytest.fixture
def reportreader():
    report = IndexReport(adsh=APPLE_ADSH_10Q_2010_Q1, cik=320193, name='APPLE INC',
                         form='10-Q', filed=20100125, period=20091231, originFile='2010q1.zip',
                         originFileType='quarter')
    yield ReportReader(report=report, zip_dir=PATH_TO_ZIP)


def test_read_raw_data(reportreader):
    reportreader._read_raw_data()

    assert reportreader.num_df.shape == (145, 9)
    assert reportreader.pre_df.shape == (100, 10)


def test_financial_statements_for_dates(reportreader):
    reportreader._read_raw_data()

    # read only for the actual period
    fin_stmts_df = reportreader._financial_statements_for_dates([reportreader.report.period])

    assert fin_stmts_df.shape == (74, 10)

    # read for the actual period and the previous period
    # 20091231 and 20081231 (sine the dates are ints, we just can remove 10_000 to get the last year
    fin_stmts_df = reportreader._financial_statements_for_dates([reportreader.report.period,
                                                                 reportreader.report.period - 10_000])

    assert fin_stmts_df.shape == (74, 11)

