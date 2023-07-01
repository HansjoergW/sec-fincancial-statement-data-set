"""
Example for CompanyCollector
"""
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag
from secfsdstools.e_collector.companycollecting import CompanyReportCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter


def run():
    # pylint: disable=W0612
    """
    run the example
    """

    # The CompanyReportCollector enables the user to read multiple reports of a
    # single company (cik number) from different zipfiles. This can be done by providing
    # a cik number and the list of forms that should be read

    # The following reader reads all available 10-K reports of apple
    company_collector = CompanyReportCollector.get_company_collector(ciks=[320193],
                                                                     forms_filter=['10-K'])
    # The interface is the same for all collectors: a simple collect() method
    # when calling the collect() method, all the available 10-K reports from apple
    # are packed into a single instance of RawDataBag
    apple_10k_bag: RawDataBag = company_collector.collect()

    # The RawDataBag contains the merged dataframes for the data from sub_txt, pre_txt, and num_txt
    raw_sub_df = apple_10k_bag.sub_df
    raw_pre_df = apple_10k_bag.pre_df
    raw_num_df = apple_10k_bag.num_df

    # On the bag itself, we can apply filters.
    # E.g., usually a report contains data from the
    # previous year and years before that. For instance, the 10-K from 2022 contains data
    # from the 10-K from 2021, and so on. However, since we anyway have collected all the
    # available reports, we only need the datapoints that are for main year of the report.

    # This can be done with the ReportPeriodRawFilter
    apple_10k_bag_main_period: RawDataBag = apple_10k_bag.filter(ReportPeriodRawFilter())

    # There are many more filters, like filtering for certain Tags, or certain statements
    # (BS, IS, CF, ..). Moreover, it is simply interface, so you can define your own filters as
    # well.

    # The pre and the num data have a strong relation. The data in the pre_txt
    # actually shows how the information of the num_text is "presented".
    # That means, that pre_txt and num_text are often being joined.

    # In order to do that, the RawDataBag class provides the get_joined_bag method
    apple_10k_bag_main_period_joined: JoinedDataBag = apple_10k_bag_main_period.join()

    # the JoinedDataBag databag contains dataframes for the sub_text and the joined pre_num_txt
    joined_sub_df = apple_10k_bag_main_period_joined.sub_df
    joined_pre_num_df = apple_10k_bag_main_period_joined.pre_num_df

    # if you want to get the same representation of the data like they appear in the real
    # financial report, then you can use the StandardStatementPresenter.
    # This creates a dataframe where you the same order of the tags how they appear in the
    # official report. Moreover, there is a column for every report date (the "period" of the
    # report.

    report_presentation_df = apple_10k_bag_main_period_joined.present(StandardStatementPresenter())

    # this is a pure pandas dataframe, so you can filter further. E.g., having a dataframes for
    # every of the main statements (Balance Sheet, Income Statement, and Cash Flow)

    # Filter for BalanceSheets
    bs_df = report_presentation_df[report_presentation_df.stmt == 'BS']

    # Filter for IncomeStatement
    is_df = report_presentation_df[report_presentation_df.stmt == 'IS']

    # Filter for CashFlow
    cf_df = report_presentation_df[report_presentation_df.stmt == 'CF']


if __name__ == '__main__':
    run()
