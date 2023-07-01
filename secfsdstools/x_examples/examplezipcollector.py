"""
Examples for ZipReportReader
"""
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.rawfiltering import StmtsRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter


def run():
    # pylint: disable=W0612,C0103
    """
    run the example
    """
    zip_file_2010q1 = "2010q1.zip"

    zip_collector = ZipCollector.get_zip_by_name(name=zip_file_2010q1)

    # The interface is the same for all collectors: a simple collect() method
    # when calling the collect() method, all reports from that quarter are loaded
    # and packed into a single instance of RawDataBag
    zip_2010q1_bag: RawDataBag = zip_collector.collect()

    # The RawDataBag contains the merged dataframes for the data from sub_txt, pre_txt, and num_txt
    raw_sub_df = zip_2010q1_bag.sub_df
    raw_pre_df = zip_2010q1_bag.pre_df
    raw_num_df = zip_2010q1_bag.num_df

    # On the bag itself, we can apply filters.
    # Maybe we are just interested in all the information of the Balance Sheets, therefore,
    # we apply a StmtsRawFilter.

    zip_2010q1_BS_bag: RawDataBag = zip_2010q1_bag.filter(StmtsRawFilter(stmts=['BS']))

    # There are many more filters, like filtering for certain Tags. Moreover, it is simply
    # interface, so you can define your own filters as well.

    # The pre and the num data have a strong relation. The data in the pre_txt
    # actually shows how the information of the num_text is "presented".
    # That means, that pre_txt and num_text are often being joined.

    # In order to do that, the RawDataBag class provides the get_joined_bag method
    zip_2010q1_BS_bag_joined: JoinedDataBag = zip_2010q1_BS_bag.join()

    # the JoinedDataBag databag contains dataframes for the sub_text and the joined pre_num_txt
    joined_sub_df = zip_2010q1_BS_bag_joined.sub_df
    joined_pre_num_df = zip_2010q1_BS_bag_joined.pre_num_df

    # if you want to get the same representation of the data like they appear in the real
    # financial report, then you can use the StandardStatementPresenter.
    # This creates a dataframe where you the same order of the tags how they appear in the
    # official report. Moreover, there is a column for every report date (the "period" of the
    # report.

    bs_report_presentation_df = zip_2010q1_BS_bag_joined.present(StandardStatementPresenter())


if __name__ == '__main__':
    run()
