""" contains collector, that reads a single report """
from typing import Optional, List

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor, IndexReport
from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_collector.basecollector import BaseCollector


class SingleReportCollector(BaseCollector):
    """
    reading the data for a single report. also provides several convenient methods
    to prepare and aggregate the raw data
    """

    @classmethod
    def get_report_by_adsh(cls, adsh: str,
                           stmt_filter: Optional[List[str]] = None,
                           tag_filter: Optional[List[str]] = None,
                           configuration: Optional[Configuration] = None):
        """
        creates the ReportReader instance for a certain adsh.
        if no configuration is passed, it reads the configuration from the configuration file

        Args:
            adsh (str): unique report id

            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)

            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)

            configuration (Configuration optional, default=None): Optional configuration object

        Returns:
            SingleReportCollector: instance of SingleReportCollector

        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)
        return SingleReportCollector.get_report_by_indexreport(
            dbaccessor.read_index_report_for_adsh(adsh=adsh),
            stmt_filter=stmt_filter,
            tag_filter=tag_filter)

    @classmethod
    def get_report_by_indexreport(cls,
                                  index_report: IndexReport,
                                  stmt_filter: Optional[List[str]] = None,
                                  tag_filter: Optional[List[str]] = None):
        """
        crates the ReportReader instance based on the IndexReport instance

        Args:
            index_report (IndexReport): instance of IndexReport

            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)

            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)

        Returns:
            SingleReportCollector: isntance of SingleReportCollector
        """
        return SingleReportCollector(report=index_report,
                                     tag_filter=tag_filter,
                                     stmt_filter=stmt_filter)

    def __init__(self,
                 report: IndexReport,
                 stmt_filter: Optional[List[str]] = None,
                 tag_filter: Optional[List[str]] = None):
        super().__init__(datapath=report.fullPath, stmt_filter=stmt_filter, tag_filter=tag_filter)
        self.report = report
        self.databag: Optional[RawDataBag] = None

    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag

        Returns:
            RawDataBag: the collected Data

        """
        adsh_filter = ('adsh', '==', self.report.adsh)
        return self.basecollect(sub_df_filter=adsh_filter)
