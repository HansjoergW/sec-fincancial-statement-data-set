"""
Examples for ReportReader
"""
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag
from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_filter.rawfiltering import TagRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter


def run():
    # pylint: disable=W0612
    """
    run the example
    """

    # id apples 10k report from september 2022
    adsh_apple_10k_2022: str = '0000320193-22-000108'

    apple_10k_2022_collector = SingleReportCollector.get_report_by_adsh(adsh=adsh_apple_10k_2022)

    # The interface is the same for all collectors: a simple collect() method
    # when calling the collect() method, the data for the selected report
    # is packed into a single instance of RawDataBag
    apple_10k_2022_bag: RawDataBag = apple_10k_2022_collector.collect()

    # The RawDataBag contains now only the information for that single reports in the dataframes
    # for sub_txt, pre_txt, and num_txt
    raw_sub_df = apple_10k_2022_bag.sub_df
    raw_pre_df = apple_10k_2022_bag.pre_df
    raw_num_df = apple_10k_2022_bag.num_df

    # On the bag itself, we can apply filters.
    # Maybe we are just interested in a few "tags".

    # This can be done with a TagRawFilter
    apple_10k_2022_tag_filtered_bag = \
        apple_10k_2022_bag.filter(TagRawFilter(tags=["Assets", "Liabilities"]))

    # There are many more filters, like filtering for certain Tags, or certain statements
    # (BS, IS, CF, ..). Moreover, it is simply interface, so you can define your own filters as
    # well.

    # The pre and the num data have a strong relation. The data in the pre_txt
    # actually shows how the information of the num_text is "presented".
    # That means, that pre_txt and num_text are often being joined.

    # In order to do that, the RawDataBag class provides the get_joined_bag method
    apple_10k_2022_bag_joined: JoinedDataBag = apple_10k_2022_bag.get_joined_bag()

    # the JoinedDataBag databag contains dataframes for the sub_text and the joined pre_num_txt
    joined_sub_df = apple_10k_2022_bag_joined.sub_df
    joined_pre_num_df = apple_10k_2022_bag_joined.pre_num_df

    # if you want to get the same representation of the data like they appear in the real
    # financial report, then you can use the StandardStatementPresenter.
    # This creates a dataframe where you the same order of the tags how they appear in the
    # official report. Moreover, there is a column for every report date (the "period" of the
    # report.

    report_presentation_df = apple_10k_2022_bag_joined.present(StandardStatementPresenter())

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
