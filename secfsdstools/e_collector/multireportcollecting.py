"""
Reads several reports from different files parallel
"""
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, List, Dict

from secfsdstools.e_collector.basecollector import BaseCollector

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.parallelexecution import ParallelExecutor
from secfsdstools.c_index.indexdataaccess import IndexReport, ParquetDBIndexingAccessor
from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_collector.zipcollecting import ZipCollector


@dataclass
class MultiReportCollector:
    """
    The MultiReport Reader can read reports from different zip files
    and provide their data in single RawDataBag.
    """

    @classmethod
    def get_reports_by_adshs(cls, adshs: List[str],
                             stmt_filter: Optional[List[str]] = None,
                             tag_filter: Optional[List[str]] = None,
                             configuration: Optional[Configuration] = None):
        """
        creates the MultiReportCollector instance for a certain list of adshs.

        if no configuration is passed, it reads the config from the config file

        Args:
            adshs (List[str]): List with unique report ids to load
            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)
            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)

            configuration (Configuration optional, default=None): Optional configuration object

        Returns:
            MultiReportCollector: instance of MultiReportCollector
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)

        index_reports = dbaccessor.read_index_reports_for_adshs(adshs=adshs)
        return MultiReportCollector(index_reports=index_reports,
                                    stmt_filter=stmt_filter,
                                    tag_filter=tag_filter)

    @classmethod
    def get_reports_by_indexreports(cls,
                                    index_reports: List[IndexReport],
                                    stmt_filter: Optional[List[str]] = None,
                                    tag_filter: Optional[List[str]] = None,
                                    ):
        """
        crates the MultiReportCollector instance based on IndexReport instances

        Args:
            index_reports (List[IndexReport]): instances of IndexReport
            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)
            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)

        Returns:
            MultiReportCollector: instance of MultiReportCollector
        """
        return MultiReportCollector(index_reports=index_reports,
                                    stmt_filter=stmt_filter,
                                    tag_filter=tag_filter)

    def __init__(self, index_reports: List[IndexReport],
                 stmt_filter: Optional[List[str]] = None,
                 tag_filter: Optional[List[str]] = None):
        super().__init__()
        self.index_reports = index_reports
        self.stmt_filter = stmt_filter
        self.tag_filter = tag_filter

    def _multi_collect(self) -> RawDataBag:
        """
        Reads the list of defined index_reports parallel and concats the content in single
        DataBag.

        Returns:
            RawDataBag: a single DataBag containing all the collected reports
        """
        # todo: consider optimization to group by the same source file
        #   and use the filter option on pd.read_parquet()
        reports: List[IndexReport] = self.index_reports

        # organize by originfile
        adshs_per_file: Dict[str, List[IndexReport]] = defaultdict(list)
        for r in reports:
            adshs_per_file[r.originFile].append(r)

        def get_entries() -> List[List[IndexReport]]:
            # the result is a list of list of IndexReports. Every IndexReport list has the same
            # originFile and therefore also the same fullPath.
            return list(adshs_per_file.values())

        def process_element(element: List[IndexReport]) -> RawDataBag:
            # the received list only contains reports that are stored in the same file, so
            # they all have the same fullPath.
            datapath = element[0].fullPath
            adshs = [x.adsh for x in element]

            collector = BaseCollector(datapath=datapath,
                                      stmt_filter=self.stmt_filter,
                                      tag_filter=self.tag_filter)

            adsh_filter = ('adsh', 'in', adshs)

            return collector.basecollect(sub_df_filter=adsh_filter)

        def post_process(parts: List[RawDataBag]) -> List[RawDataBag]:
            # do nothing
            return parts

        execute_serial = False
        if len(self.index_reports) == 1:
            execute_serial = True
        executor = ParallelExecutor(chunksize=0, execute_serial=execute_serial)

        executor.set_get_entries_function(get_entries)
        executor.set_process_element_function(process_element)
        executor.set_post_process_chunk_function(post_process)

        # we ignore the missing, since get_entries always returns the whole list
        collected_reports: List[RawDataBag]
        collected_reports, _ = executor.execute()

        return RawDataBag.concat(collected_reports)

    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag

        Returns:
            RawDataBag: the collected Data
        """
        return self._multi_collect()