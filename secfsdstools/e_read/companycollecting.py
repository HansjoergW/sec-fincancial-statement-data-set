"""
Collects all data for a company.
"""
from collections import defaultdict
from typing import Dict, Optional, List

import pandas as pd

from secfsdstools.a_config.configmgt import Configuration, ConfigurationManager
from secfsdstools.a_utils.parallelexecution import ParallelExecutor
from secfsdstools.d_index.indexdataaccess import DBIndexingAccessor, IndexReport
from secfsdstools.e_read.basereportreading import NUM_TXT, PRE_TXT, SUB_TXT, BaseReportReader
from secfsdstools.e_read.reportreading import ReportReader


class CompanyCollector(BaseReportReader):

    @classmethod
    def get_company_collector(cls, cik: int, forms: Optional[List[str]] = None, configuration: Optional[Configuration] = None):
        """
        creates a company_collector instance for the provided cik. If no configuration object is passed,
        it reads the configuration from the config file.

        Args:
            cik (int): the central identification key which is assigned by the sec for every company
            forms: a list of forms which should be collected, like 10-K, or 10 Q
            configuration (Configuration, optional, None): Optional configuration object

        Returns:
            CompanyCollector: instance of Company Collector
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()
        return CompanyCollector(cik, forms=forms, dbaccessor=DBIndexingAccessor(db_dir=configuration.db_dir))

    def __init__(self, cik: int, dbaccessor: DBIndexingAccessor, forms: Optional[List[str]] = None):
        super().__init__()
        self.cik = cik
        self.dbaccessor = dbaccessor
        self.forms = forms

        self.collected_data: Dict[str, pd.DataFrame] = {}

    def _collect(self) -> Dict[str, pd.DataFrame]:
        """
        Collects

        Returns:
            Dict[str, pd.DataFrame]: key is file (Sub, Num, Pre) value is the content as pd.DataFrame
        """

        # Achtung: reports können in mehreren Zips stehen - daily oder Quarter
        reports: List[IndexReport] = self.dbaccessor.read_index_reports_for_cik(self.cik, self.forms)

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

        result, _ = executor.execute() # we ignore the missing, since get_entries always returns the whole list

        return result[0]

    def _read_raw_data(self):
        self.collected_data = self._collect()
        super()._read_raw_data()

    def _read_df_from_raw(self, file_type: str) -> pd.DataFrame:
        return self.collected_data[file_type]

