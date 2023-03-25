"""
Examples for MultiReportreader
"""
from secfsdstools.e_read.multireportreading import MultiReportReader

if __name__ == '__main__':
    # The MultiReportreader enables the user to read multiple reports from different zipfiles.
    # This can either be done by providing a list of adsh numbers or by a list if IndexReport
    #  instances

    # the following reader reads the 10-K reports of apple from the years 2020,2021, and 2022
    multi_report_reader = MultiReportReader.get_reports_by_adshs(
        adshs=['0000320193-22-000108', '0000320193-21-000105', '0000320193-20-000096'])

    # the interface is the same was with the standard reader

    # this returns a list all reports that are read by the reader
    raw_sub_df = multi_report_reader.get_raw_sub_data()
    # we expect 3 reports
    assert len(raw_sub_df) == 3

    # reading the raw content of the num and pre files
    raw_pre_df = multi_report_reader.get_raw_pre_data()
    raw_num_df = multi_report_reader.get_raw_num_data()

    # it is the same interface as the other readers. So we return a filtered dataset only containing
    # the data which are valid at the period-date of the report or the period-date of the report and
    # the previous year

    # merging the data from num and pre together and produce the primary financial statements
    # The first dataframe set contains just the data that are valid for the current year
    current_year_df = multi_report_reader.financial_statements_for_period()

    # The second dataframe contains the data that are for the current and the previous year
    current_and_previous_year_df = \
        multi_report_reader.financial_statements_for_period_and_previous_period()

    # Filter for BalanceSheets
    bs_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'BS']

    # Filter for IncomeStatement
    is_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'IS']

    # Filter for CashFlow
    cf_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'CF']
