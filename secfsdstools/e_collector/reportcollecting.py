""" contains collector, that reads a single report """
from typing import Optional, Dict, List, Union

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.constants import NUM_TXT, PRE_TXT, SUB_TXT
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
        if no configuration is passed, it reads the config from the config file

        Args:
            adsh (str): unique report id
            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)
            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)
            configuration (Configuration optional, default=None): Optional configuration object

        Returns:
            ReportReader: instance of ReportReader

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
            ReportReader: isntance of ReportReader
        """
        return SingleReportCollector(report=index_report,
                                     tag_filter=tag_filter,
                                     stmt_filter=stmt_filter)

    def __init__(self,
                 report: IndexReport,
                 stmt_filter: Optional[List[str]] = None,
                 tag_filter: Optional[List[str]] = None):
        super().__init__()
        self.report = report
        self.databag: Optional[RawDataBag] = None
        self.stmt_filter = stmt_filter
        self.tag_filter = tag_filter

    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag

        Returns:
            RawDataBag: the collected Data

        """
        if self.databag is None:
            adsh_filter = ('adsh', '==', self.report.adsh)
            sub_df = self._read_df_from_raw_parquet(file=SUB_TXT,
                                                    path=self.report.fullPath,
                                                    filters=[adsh_filter])

            pre_filter, num_filter = self._get_pre_num_filters(adshs=[self.report.adsh],
                                                               stmts=self.stmt_filter,
                                                               tags=self.tag_filter)

            pre_df = self._read_df_from_raw_parquet(
                file=PRE_TXT, path=self.report.fullPath, filters=pre_filter if pre_filter else None
            )

            num_df = self._read_df_from_raw_parquet(
                file=NUM_TXT, path=self.report.fullPath, filters=num_filter if num_filter else None
            )

            self.databag = RawDataBag.create(sub_df=sub_df, pre_df=pre_df, num_df=num_df)

        return self.databag

    def submission_data(self) -> Dict[str, Union[str, int]]:
        """
        returns the data from the submission txt file as dictionary

        Returns:
            Dict[str, Union[str, int]]: data from submission txt file as dictionary
        """
        databag = self.collect()
        return databag.sub_df.loc[0].to_dict()
