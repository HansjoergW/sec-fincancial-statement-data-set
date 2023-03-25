"""
Examples for ReportReader
"""

from secfsdstools.e_read.reportreading import ReportReader


def run():
    # pylint: disable=W0612
    """
    run the example
    """

    # id apples 10k report from september 2022
    adsh_apple_10k_2022: str = '0000320193-22-000108'

    apple_10k_2022_reader = ReportReader.get_report_by_adsh(adsh=adsh_apple_10k_2022)

    # reading the raw content of the num and pre files
    raw_pre_df = apple_10k_2022_reader.get_raw_pre_data()
    raw_num_df = apple_10k_2022_reader.get_raw_num_data()

    # just merge the data of the num and pre dataframes, without pivoting the data
    # -> the ddate stays as column
    # setting the use_period parameter to true, we will just keep the data for the current year.
    # if we also set the use_previous_period parameter to True, we would also keep the data
    # of the previous year.
    apple_10k_2020_current_year_merged = apple_10k_2022_reader.merge_pre_and_num(use_period=True)


    # merging the data from num and pre together and produce the primary financial statements
    apple_10k_2022_current_year_df = apple_10k_2022_reader.financial_statements_for_period()
    apple_10k_2022_current_and_previous_year_df = \
        apple_10k_2022_reader.financial_statements_for_period_and_previous_period()

    # Filter for BalanceSheet
    apple_10k_2022_bs_df = apple_10k_2022_current_and_previous_year_df[
        apple_10k_2022_current_and_previous_year_df.stmt == 'BS']

    # Filter for IncomeStatement
    apple_10k_2022_is_df = apple_10k_2022_current_and_previous_year_df[
        apple_10k_2022_current_and_previous_year_df.stmt == 'IS']

    # Filter for CashFlow
    apple_10k_2022_cf_df = apple_10k_2022_current_and_previous_year_df[
        apple_10k_2022_current_and_previous_year_df.stmt == 'CF']


if __name__ == '__main__':
    run()
