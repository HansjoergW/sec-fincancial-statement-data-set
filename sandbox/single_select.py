from secfsdstools.e_collector.reportcollecting import SingleReportCollector

from secfsdstools.e_filter.joinedfiltering import ReportPeriodJoinedFilter

if __name__ == '__main__':
    collector = SingleReportCollector.get_report_by_adsh(adsh="0001144204-11-063618", stmt_filter=['IS'])

    bag = collector.collect()

    joined = bag.join()[ReportPeriodJoinedFilter()]


    print("test")