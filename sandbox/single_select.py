from secfsdstools.e_collector.reportcollecting import SingleReportCollector

from secfsdstools.e_filter.joinedfiltering import ReportPeriodJoinedFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter

if __name__ == '__main__':
    collector = SingleReportCollector.get_report_by_adsh(adsh="0000002969-20-000019", stmt_filter=['IS'])

    bag = collector.collect()

    joined = bag.join() #[ReportPeriodJoinedFilter()]

    presentation = joined.present(StandardStatementPresenter())


    print("test")