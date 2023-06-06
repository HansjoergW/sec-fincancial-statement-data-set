import os

import pytest
from secfsdstools.e_read.zipreportreading import ZipReportReader

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = f'{CURRENT_DIR}/testdataparquet/quarter/2010q1.zip'


@pytest.fixture
def zipreader():
    zipreader = ZipReportReader(datapath=PATH_TO_ZIP)
    zipreader._read_raw_data()
    return zipreader


def test_financial_statements_for_tags(zipreader):
    # read only for the actual period
    fin_stmts_df = zipreader.financial_statements_for_period()

    assert fin_stmts_df.shape == (61245, 18)

    # read for the actual period and the previous period
    fin_stmts_df = zipreader.financial_statements_for_period_and_previous_period()

    assert fin_stmts_df.shape == (62255, 25)

    # read only for the actual period
    fin_stmts_df = zipreader.financial_statements_for_period(tags=['Assets'])
    assert fin_stmts_df.shape == (513, 18)
