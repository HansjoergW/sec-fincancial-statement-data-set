"""
Examples for MultiReportreader
"""
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag
from secfsdstools.e_collector.multireportcollecting import MultiReportCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, StmtRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter


def run():
    # pylint: disable=W0612
    """
    run the example
    """

    # The MultiReportCollector enables the user to read multiple reports from different zipfiles.
    # This can either be done by providing a list of adsh numbers or by a list if IndexReport
    #  instances

    # the following reader reads the 10-K reports of apple from the years 2020,2021, and 2022
    multi_report_collector = MultiReportCollector.get_reports_by_adshs(
        adshs=['0000320193-22-000108', '0000320193-21-000105', '0000320193-20-000096'])

    # The interface is the same for all collectors: a simple collect() method
    # when calling the collect() method, all defined reports are
    # packed into a single instance of RawDataBag
    apple_selected_10ks_bag: RawDataBag = multi_report_collector.collect()

    # The RawDataBag contains the merged dataframes for the data from sub_txt, pre_txt, and num_txt
    raw_sub_df = apple_selected_10ks_bag.sub_df
    raw_pre_df = apple_selected_10ks_bag.pre_df
    raw_num_df = apple_selected_10ks_bag.num_df

    # On the bag itself, we can apply filters and can also chain them
    # E.g., usually a report contains data from the
    # previous year and years before that. For instance, the 10-K from 2022 contains data
    # from the 10-K from 2021, and so on. Maybe we just want to have the data for the period
    # of the reports, so we apply the ReportPeriodRawFilter. Moreover, maybe we are just interested
    # in the Balance Sheet and the Cash Flow, so we apply the StmtRawFilter filter as well
    apple_selected_10ks_filtered_bag: RawDataBag = (
        apple_selected_10ks_bag
        .filter(ReportPeriodRawFilter())
        .filter(StmtRawFilter(stmts=['BS', 'CF'])))

    # There are many more filters, like filtering for certain Tags.
    # Moreover, it is simply interface, so you can define your own filters as well.

    # The pre and the num data have a strong relation. The data in the pre_txt
    # actually shows how the information of the num_text is "presented".
    # That means, that pre_txt and num_text are often being joined.

    # In order to do that, the RawDataBag class provides the get_joined_bag method
    apple_selected_10ks_bag_filtered_joined: JoinedDataBag = \
        apple_selected_10ks_filtered_bag.join()

    # the JoinedDataBag databag contains dataframes for the sub_text and the joined pre_num_txt
    joined_sub_df = apple_selected_10ks_bag_filtered_joined.sub_df
    joined_pre_num_df = apple_selected_10ks_bag_filtered_joined.pre_num_df

    # if you want to get the same representation of the data like they appear in the real
    # financial report, then you can use the StandardStatementPresenter.
    # This creates a dataframe where you the same order of the tags how they appear in the
    # official report. Moreover, there is a column for every report date (the "period" of the
    # report.

    report_presentation_df = \
        apple_selected_10ks_bag_filtered_joined.present(StandardStatementPresenter())

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
