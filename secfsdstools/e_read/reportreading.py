"""
reading the data for a single report.
"""
import os
import re
from dataclasses import dataclass
from typing import Optional, List, Dict, Union

import numpy as np
import pandas as pd

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexReport, create_index_accessor
from secfsdstools.e_read.basereportreading import BaseReportReader


@dataclass
class BasicReportStats:
    """
    Contains simple statistics of a report.
    """
    num_entries: int
    pre_entries: int
    facts_per_date: Dict[int, int]
    list_of_statements: List[str]
    tags_per_statement: Dict[str, List[str]]


class ReportReader(BaseReportReader):
    """
    reading the data for a single report. also provides several convenient methods
    to prepare and aggregate the raw data
    """

    @classmethod
    def get_report_by_adsh(cls, adsh: str, configuration: Optional[Configuration] = None):
        """
        creates the ReportReader instance for a certain adsh.
        if no configuration is passed, it reads the config from the config file

        Args:
            adsh (str): unique report id
            configuration (Configuration optional, default=None): Optional configuration object

        Returns:
            ReportReader: instance of ReportReader

        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = create_index_accessor(db_dir=configuration.db_dir)
        return ReportReader.get_report_by_indexreport(
            dbaccessor.read_index_report_for_adsh(adsh=adsh))

    @classmethod
    def get_report_by_indexreport(cls, index_report: IndexReport):
        """
        crates the ReportReader instance based on the IndexReport instance

        Args:
            index_report (IndexReport): instance of IndexReport

        Returns:
            ReportReader: isntance of ReportReader
        """
        return ReportReader(index_report)

    def __init__(self, report: IndexReport):
        super().__init__()
        self.report = report

    def _read_df_from_raw(self,
                          file: str) -> pd.DataFrame:
        return self._read_df_from_raw_parquet(file)

    def _read_df_from_raw_parquet(self,
                                  file: str) -> pd.DataFrame:
        return pd.read_parquet(os.path.join(self.report.fullPath, f'{file}.parquet'),
                               filters=[('adsh', '==', self.report.adsh)])

    def submission_data(self) -> Dict[str, Union[str, int]]:
        """
        returns the data from the submission txt file as dictionary

        Returns:
            Dict[str, Union[str, int]]: data from submission txt file as dictionary
        """
        self._read_raw_data()  # lazy load the data if necessary
        return self.sub_df.loc[0].to_dict()

    def statistics(self) -> BasicReportStats:
        """
        calculate a few simple statistics of a report.
        - number of entries in the num-file
        - number of entries in the pre-file
        - number of facts per ddate (in num file)
        - list of different statements in the pre file
        - list of tags per statement

        Rreturns:
            BasicReportsStats: instance with basic report infos
        """

        self._read_raw_data()  # lazy load the data if necessary
        num_entries = len(self.num_df)
        pre_entries = len(self.pre_df)
        facts_per_date: Dict[int, int] = self.num_df.ddate.value_counts().to_dict()
        list_of_statements: List[str] = self.pre_df.stmt.unique().tolist()
        tags_per_statement_raw: Dict[str, np.array] = \
            self.pre_df[['stmt', 'tag']].groupby('stmt')['tag'].unique().to_dict()
        tags_per_statement: Dict[str, List[str]] = \
            {k: v.tolist() for k, v in tags_per_statement_raw.items()}

        return BasicReportStats(num_entries=num_entries,
                                pre_entries=pre_entries,
                                facts_per_date=facts_per_date,
                                list_of_statements=list_of_statements,
                                tags_per_statement=tags_per_statement)
