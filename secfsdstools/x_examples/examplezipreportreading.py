"""
Examples for ZipReportReader
"""
from secfsdstools.e_read.zipreportreading import ZipReportReader


def run():
    # pylint: disable=W0612
    """
    run the example
    """
    zip_file_2010q1 = "2010q1.zip"

    zip_report_reader = ZipReportReader.get_zip_by_name(name=zip_file_2010q1)

    # reading the raw content of the sub file
    # this returns a list all reports that are read by the reader
    raw_sub_df = zip_report_reader.get_raw_sub_data()

    # reading the raw content of the num and pre files
    raw_pre_df = zip_report_reader.get_raw_pre_data()
    raw_num_df = zip_report_reader.get_raw_num_data()

    # merging the data from num and pre together and produce the primary financial statements
    current_year_df = zip_report_reader.financial_statements_for_period()
    current_and_previous_year_df = \
        zip_report_reader.financial_statements_for_period_and_previous_period()

    # Filter for BalanceSheets
    bs_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'BS']

    # Filter for IncomeStatement
    is_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'IS']

    # Filter for CashFlow
    cf_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'CF']


if __name__ == '__main__':
    run()
