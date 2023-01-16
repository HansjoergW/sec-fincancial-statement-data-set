import os

import pytest

from secfsdstools.e_read.zipreportreading import ZipReportReader

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = CURRENT_DIR + '/testdata/2010q1.zip'


@pytest.fixture
def zipreader():
    zipreader = ZipReportReader(zipfile=PATH_TO_ZIP)
    zipreader._read_raw_data()
    return zipreader


def test_read_raw_data(zipreader):
    assert zipreader.get_raw_num_data().shape == (151692, 9)
    assert zipreader.get_raw_pre_data().shape == (88378, 10)
    assert zipreader.get_raw_sub_data().shape == (495, 36)


def test_financial_statements_for_tags(zipreader):
    # read only for the actual period
    fin_stmts_df = zipreader.financial_statements_for_period()

    assert fin_stmts_df.shape == (59521, 17)

    # read for the actual period and the previous period
    fin_stmts_df = zipreader.financial_statements_for_period_and_previous_period()

    assert fin_stmts_df.shape == (60515, 24)

    # read only for the actual period
    fin_stmts_df = zipreader.financial_statements_for_period(tags=['Assets'])
    assert fin_stmts_df.shape == (496, 17)


def test_statistics(zipreader):
    stats = zipreader.statistics()

    assert stats.num_entries == 151692
    assert stats.pre_entries == 88378
    assert stats.number_of_reports == 495
    assert stats.reports_per_form['10-Q'] == 80
    assert stats.reports_per_period_date[20091231] == 434
