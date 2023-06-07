import os

APPLE_ADSH_10Q_2010_Q1 = '0001193125-10-012085'
APPLE_ADSH_10Q_2010_Q2 = '0001193125-10-088957'

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_PARQUET_Q1 = f'{CURRENT_DIR}/testdataparquet/quarter/2010q1.zip'
PATH_TO_PARQUET_Q2 = f'{CURRENT_DIR}/testdataparquet/quarter/2010q2.zip'


def test_financial_statements(multireportreaderparquet):
    # read only for the actual period
    fin_stmts_df = multireportreaderparquet.financial_statements_for_period()

    assert fin_stmts_df.shape == (150, 13)

    # read for the actual period and the previous period
    fin_stmts_df = multireportreaderparquet.financial_statements_for_period_and_previous_period()

    assert fin_stmts_df.shape == (150, 15)

    # read only for the actual period
    fin_stmts_df = multireportreaderparquet.financial_statements_for_period(tags=['Assets'])
    assert fin_stmts_df.shape == (2, 13)
