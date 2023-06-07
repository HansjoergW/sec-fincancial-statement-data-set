"""
Reads several reports from different files parallel
"""
from dataclasses import dataclass
from typing import Dict, Optional, List

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.parallelexecution import ParallelExecutor
from secfsdstools.c_index.indexdataaccess import IndexReport, create_index_accessor
from secfsdstools.d_container.databagmodel import DataBag, concat
from secfsdstools.e_collector.reportcollecting import SingleReportCollector


@dataclass
class MultiReportStats:
    """
    Contains simple statistics of a report.
    """
    num_entries: int
    pre_entries: int
    number_of_reports: int
    reports_per_form: Dict[str, int]
    reports_per_period_date: Dict[int, int]


@dataclass
class MultiReportCollector:
    """
    The MultiReport Reader can read reports from different zip files
    and provide their data in single DataBag.
    """

    @classmethod
    def get_reports_by_adshs(cls, adshs: List[str], configuration: Optional[Configuration] = None):
        """
        creates the MultiReportCollector instance for a certain list of adshs.

        if no configuration is passed, it reads the config from the config file

        Args:
            adshs (List[str]): List with unique report ids to load
            configuration (Configuration optional, default=None): Optional configuration object

        Returns:
            MultiReportCollector: instance of MultiReportCollector
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = create_index_accessor(db_dir=configuration.db_dir)

        index_reports = dbaccessor.read_index_reports_for_adshs(adshs=adshs)
        return MultiReportCollector(index_reports)

    @classmethod
    def get_reports_by_indexreport(cls, index_reports: List[IndexReport]):
        """
        crates the MultiReportCollector instance based on IndexReport instances

        Args:
            index_reports (List[IndexReport]): instances of IndexReport

        Returns:
            MultiReportCollector: instance of MultiReportCollector
        """
        return MultiReportCollector(index_reports)

    def __init__(self, index_reports: List[IndexReport]):
        super().__init__()
        self.index_reports = index_reports

        self.databag: Optional[DataBag] = None

    def _collect(self) -> DataBag:
        """
        Reads the list of defined index_reports parallel and concats the content in single
        DataBag.

        Returns:
            DataBag: a single DataBag containing all the collected reports
        """
        # todo: consider optimization to group by the same source file
        #   and use the filter option on pd.read_parquet()
        reports: List[IndexReport] = self.index_reports

        def get_entries() -> List[IndexReport]:
            return reports

        def process_element(element: IndexReport) -> DataBag:
            print(element.adsh)
            collector = SingleReportCollector.get_report_by_indexreport(index_report=element)

            return collector.collect()

        def post_process(parts: List[DataBag]) -> List[DataBag]:
            # do nothing
            return parts

        executor = ParallelExecutor(chunksize=0)

        executor.set_get_entries_function(get_entries)
        executor.set_process_element_function(process_element)
        executor.set_post_process_chunk_function(post_process)

        # we ignore the missing, since get_entries always returns the whole list
        collected_reports: List[DataBag]
        collected_reports, _ = executor.execute()

        return concat(collected_reports)

    def collect(self) -> DataBag:
        """
        collects the data and returns a Databag

        Returns:
            DataBag: the collected Data
        """
        if self.databag is None:
            self.databag = self._collect()
        return self.databag

    def statistics(self) -> MultiReportStats:
        """
        calculate a few simple statistics of a report.
        - number of entries in the num-file
        - number of entries in the pre-file
        - number of reports in the zip-file (equals number of entries in sub-file)
        - number of reports per form (10-K, 10-Q, ...)
        - number of reports per period date (counts per value in the period column of sub-file)

        Rreturns:
            ZipFileStats: instance with basic report infos
        """
        databag = self.collect()

        num_entries = len(databag.num_df)
        pre_entries = len(databag.pre_df)
        number_of_reports = len(databag.sub_df)
        reports_per_period_date: Dict[int, int] = databag.sub_df.period.value_counts().to_dict()
        reports_per_form: Dict[str, int] = databag.sub_df.form.value_counts().to_dict()

        return MultiReportStats(num_entries=num_entries,
                                pre_entries=pre_entries,
                                number_of_reports=number_of_reports,
                                reports_per_form=reports_per_form,
                                reports_per_period_date=reports_per_period_date
                                )
