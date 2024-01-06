from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer
from secfsdstools.u_usecases.bulk_loading import default_postloadfilter

if __name__ == '__main__':
    report = SingleReportCollector.get_report_by_adsh(adsh='0001493152-20-015569',
                                                      stmt_filter=['BS'])
    bag = report.collect()
    filtered_bag = default_postloadfilter(bag)
    joined_bag = filtered_bag.join()

    bs_std = BalanceSheetStandardizer()
    result = bs_std.present(joined_bag)
    print(result)
