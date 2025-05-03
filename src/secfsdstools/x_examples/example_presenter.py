"""
Simple code example on how to use the StandardStatementPresenter
"""

import pandas as pd

from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodAndPreviousPeriodRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def presenter():
    """ StandardStatementPresenter example"""

    apple_10k_2022_adsh = "0000320193-22-000108"

    collector: SingleReportCollector = SingleReportCollector.get_report_by_adsh(
        adsh=apple_10k_2022_adsh,
        stmt_filter=["BS"]
    )
    rawdatabag = collector.collect()
    bs_df = (rawdatabag
             .filter(ReportPeriodAndPreviousPeriodRawFilter())
             .join()
             .present(StandardStatementPresenter()))
    print(bs_df)


def run():
    """launch method"""

    presenter()


if __name__ == '__main__':
    run()
