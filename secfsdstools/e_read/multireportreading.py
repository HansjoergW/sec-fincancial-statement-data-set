"""
Reads several reports from different files parallel
"""
from dataclasses import dataclass
from typing import Dict, Optional, List

import pandas as pd

from secfsdstools.a_config.configmgt import Configuration, ConfigurationManager
from secfsdstools.a_utils.parallelexecution import ParallelExecutor
from secfsdstools.d_index.indexdataaccess import DBIndexingAccessor, IndexReport
from secfsdstools.e_read.basereportreading import BaseReportReader, SUB_TXT, PRE_TXT, NUM_TXT
from secfsdstools.e_read.reportreading import ReportReader


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


class MultiReportReader(BaseReportReader):
    """
    The MultiReport Reader can read reports from different zip files
    and provide their data in single pre, num, and sub DataFrames.


    """

    @classmethod
    def get_reports_by_adshs(cls, adshs: List[str], configuration: Optional[Configuration] = None):
        """
        creates the MultiReportReader instance for a certain list of adshs.

        if no configuration is passed, it reads the config from the config file

        Args:
            adshs (List[str]): List with unique report ids to load
            configuration (Configuration optional, default=None): Optional configuration object

        Returns:
            MultiReportReader: instance of MultiReportReader
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = DBIndexingAccessor(db_dir=configuration.db_dir)

        index_reports = dbaccessor.read_index_reports_for_adshs(adshs=adshs)
        return MultiReportReader(index_reports)

    @classmethod
    def get_reports_by_indexreport(cls, index_reports: List[IndexReport]):
        """
        crates the MultiReportReader instance based on IndexReport instances

        Args:
            index_reports (List[IndexReport]): instances of IndexReport

        Returns:
            MultiReportReader: isntance of MultiReportReader
        """
        return MultiReportReader(index_reports)

    def __init__(self, index_reports: List[IndexReport]):
        super().__init__()
        self.index_reports = index_reports

        self.collected_data: Dict[str, pd.DataFrame] = {}

    def _collect(self) -> Dict[str, pd.DataFrame]:
        """
        Reads the list of defined index_reports parallel and concats the content in single
        SUB, NUM, and PRE DataFrames.

        Returns:
            Dict[str, pd.DataFrame]: key is file (Sub, Num, Pre) value
            is the content as pd.DataFrame
        """

        reports: List[IndexReport] = self.index_reports

        def get_entries() -> List[IndexReport]:
            return reports

        def process_element(element: IndexReport) -> Dict[str, pd.DataFrame]:
            print(element.adsh)
            reader = ReportReader.get_report_by_indexreport(index_report=element)
            pre_df = reader.get_raw_pre_data()
            num_df = reader.get_raw_num_data()
            sub_df = reader.get_raw_sub_data()

            return {SUB_TXT: sub_df,
                    PRE_TXT: pre_df,
                    NUM_TXT: num_df}

        def post_process(parts: List[Dict[str, pd.DataFrame]]) -> List[Dict[str, pd.DataFrame]]:
            keys = parts[0].keys()
            concated_frames: Dict[str, pd.DataFrame] = {}

            for key in keys:
                concated_frames[key] = pd.concat([x[key] for x in parts]).reset_index()

            return [concated_frames]

        executor = ParallelExecutor(chunksize=0)

        executor.set_get_entries_function(get_entries)
        executor.set_process_element_function(process_element)
        executor.set_post_process_chunk_function(post_process)

        # we ignore the missing, since get_entries always returns the whole list
        result, _ = executor.execute()

        return result[0]

    def _read_raw_data(self):
        # we need to overwrite the base class methods, since we first need to read the data
        if self.num_df is None:
            self.collected_data = self._collect()
            super()._read_raw_data()

    def _read_df_from_raw(self, file_type: str) -> pd.DataFrame:
        # we need to overwrite this method, since files are not read directly from the file,
        # but were loaded in a previous step
        return self.collected_data[file_type]

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

        self._read_raw_data()  # lazy load the data if necessary
        num_entries = len(self.num_df)
        pre_entries = len(self.pre_df)
        number_of_reports = len(self.sub_df)
        reports_per_period_date: Dict[int, int] = self.sub_df.period.value_counts().to_dict()
        reports_per_form: Dict[str, int] = self.sub_df.form.value_counts().to_dict()

        return MultiReportStats(num_entries=num_entries,
                                pre_entries=pre_entries,
                                number_of_reports=number_of_reports,
                                reports_per_form=reports_per_form,
                                reports_per_period_date=reports_per_period_date
                                )
