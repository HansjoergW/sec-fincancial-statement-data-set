""" contains collector, that reads a single report """
import os
from dataclasses import dataclass
from typing import Optional, Dict, List, Union

import numpy as np
import pandas as pd

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.constants import NUM_TXT, PRE_TXT, SUB_TXT
from secfsdstools.c_index.indexdataaccess import create_index_accessor, IndexReport
from secfsdstools.d_container.databagmodel import DataBag


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


class SingleReportCollector:
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
        return SingleReportCollector.get_report_by_indexreport(
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
        return SingleReportCollector(index_report)

    def __init__(self, report: IndexReport):
        super().__init__()
        self.report = report
        self.databag: Optional[DataBag] = None

    def _read_df_from_raw_parquet(self,
                                  file: str) -> pd.DataFrame:
        return pd.read_parquet(os.path.join(self.report.fullPath, f'{file}.parquet'),
                               filters=[('adsh', '==', self.report.adsh)])

    def collect(self) -> DataBag:
        """
        collects the data and returns a Databag

        Returns:
            DataBag: the collected Data

        """
        if self.databag is None:
            num_df = self._read_df_from_raw_parquet(file=NUM_TXT)
            pre_df = self._read_df_from_raw_parquet(file=PRE_TXT)
            sub_df = self._read_df_from_raw_parquet(file=SUB_TXT)

            self.databag = DataBag.create(sub_df=sub_df, pre_df=pre_df, num_df=num_df)
        return self.databag

    def submission_data(self) -> Dict[str, Union[str, int]]:
        """
        returns the data from the submission txt file as dictionary

        Returns:
            Dict[str, Union[str, int]]: data from submission txt file as dictionary
        """
        databag = self.collect()
        return databag.sub_df.loc[0].to_dict()

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
        databag = self.collect()

        num_entries = len(databag.num_df)
        pre_entries = len(databag.pre_df)
        facts_per_date: Dict[int, int] = databag.num_df.ddate.value_counts().to_dict()
        list_of_statements: List[str] = databag.pre_df.stmt.unique().tolist()

        tags_per_statement_raw: Dict[str, np.array] = \
            databag.pre_df[['stmt', 'tag']].groupby('stmt')['tag'].unique().to_dict()
        tags_per_statement: Dict[str, List[str]] = \
            {k: v.tolist() for k, v in tags_per_statement_raw.items()}

        return BasicReportStats(num_entries=num_entries,
                                pre_entries=pre_entries,
                                facts_per_date=facts_per_date,
                                list_of_statements=list_of_statements,
                                tags_per_statement=tags_per_statement)
