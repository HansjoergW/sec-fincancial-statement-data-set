"""
Example for CompanyCollector
"""
from secfsdstools.e_read.companycollecting import CompanyReportCollector

if __name__ == '__main__':
    # The CompanyReportCollector enables the user to read multiple reports of the
    # same company (cik number) from different zipfiles. This can be done by providing
    # a cik number and the list of forms that should be read

    # the following reader reads all available 10-K reports of apple
    company_collector = CompanyReportCollector.get_company_collector(cik=320193, forms=['10-K'])

    # the interface is the same was with the standard reader

    # this returns a list all reports that are read by the reader
    raw_sub_df = company_collector.get_raw_sub_data()

    # reading the raw content of the num and pre files
    raw_pre_df = company_collector.get_raw_pre_data()
    raw_num_df = company_collector.get_raw_num_data()

    # it is the same interface as the other readers. So we return a filtered dataset
    # only containing the data which are valid at the period-date of the report or
    # the period-date of the report and the previous year

    # merging the data from num and pre together and produce the primary financial statements
    # The first dataframe set contains just the data that are valid for the year of the report
    current_year_df = company_collector.financial_statements_for_period()

    # The second dataframe contains the data that are for the current and the previous year
    current_and_previous_year_df = \
        company_collector.financial_statements_for_period_and_previous_period()

    # Filter for BalanceSheets
    bs_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'BS']

    # Filter for IncomeStatement
    is_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'IS']

    # Filter for CashFlow
    cf_df = current_and_previous_year_df[
        current_and_previous_year_df.stmt == 'CF']

    # the previous methods returned the data pivoted, meaning every all report "period"
    # dates appear as columns for certain analysis, it might be preferred to receive keep
    # the date as a value in the column. this can be done using just the merge method
    current_year_unpivoted_df = company_collector.merge_pre_and_num(use_period=True)

    # for instance, get just the Assets Tag for all years
    assets_value_for_all_years_df = \
        current_year_unpivoted_df[current_year_unpivoted_df.tag == 'Assets'].sort_values(['ddate'])

    # let's see, how the Value for Assets has increased over the years
    print(assets_value_for_all_years_df.value.tolist())
