import os

import pytest

from secfsdstools.d_index.indexdataaccess import IndexReport
from secfsdstools.e_read.multireportreading import MultiReportReader

APPLE_ADSH_10Q_2010_Q1 = '0001193125-10-012085'
APPLE_ADSH_10Q_2010_Q2 = '0001193125-10-088957'

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP_Q1 = CURRENT_DIR + '/testdata/2010q1.zip'
PATH_TO_ZIP_Q2 = CURRENT_DIR + '/testdata/2010q2.zip'


@pytest.fixture
def multireportreader():
    report1 = IndexReport(adsh=APPLE_ADSH_10Q_2010_Q1, cik=320193, name='APPLE INC',
                          form='10-Q', filed=20100125, period=20091231, originFile='2010q1.zip',
                          originFileType='quarter', fullPath=PATH_TO_ZIP_Q1, url='')
    report2 = IndexReport(adsh=APPLE_ADSH_10Q_2010_Q2, cik=320193, name='APPLE INC',
                          form='10-Q', filed=20100421, period=20100331, originFile='2010q2.zip',
                          originFileType='quarter', fullPath=PATH_TO_ZIP_Q2, url='')

    reports = [report1, report2]

    reader = MultiReportReader.get_reports_by_indexreport(index_reports=reports)
    reader._read_raw_data()
    return reader


def test_read_raw_data(multireportreader):
    assert multireportreader.sub_df.shape == (2, 37)
    assert multireportreader.num_df.shape == (321, 10)
    assert multireportreader.pre_df.shape == (211, 11)


def test_financial_statements(multireportreader):
    # read only for the actual period
    fin_stmts_df = multireportreader.financial_statements_for_period()

    assert fin_stmts_df.shape == (150, 13)

    # read for the actual period and the previous period
    fin_stmts_df = multireportreader.financial_statements_for_period_and_previous_period()

    assert fin_stmts_df.shape == (150, 15)

    # read only for the actual period
    fin_stmts_df = multireportreader.financial_statements_for_period(tags=['Assets'])
    assert fin_stmts_df.shape == (2, 13)


def test_statistics(multireportreader):
    stats = multireportreader.statistics()

    assert stats.number_of_reports == 2
    assert stats.num_entries == 321
    assert stats.pre_entries == 211
    assert stats.reports_per_form['10-Q'] == 2
    assert stats.reports_per_period_date[20091231] == 1
    assert stats.reports_per_period_date[20100331] == 1

