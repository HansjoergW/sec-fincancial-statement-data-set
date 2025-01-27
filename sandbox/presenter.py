from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter

if __name__ == '__main__':
    adsh = "0000320193-22-000108"
    stmts = ["BS"]
    reader = SingleReportCollector.get_report_by_adsh(adsh=adsh, stmt_filter=stmts)
    raw_data = reader.collect()

    filterd_data = raw_data.filter(ReportPeriodRawFilter())

    joined_df = filterd_data.join()
    report_data = joined_df.present(StandardStatementPresenter(invert_negating=True, show_segments=True))

    print(report_data)
