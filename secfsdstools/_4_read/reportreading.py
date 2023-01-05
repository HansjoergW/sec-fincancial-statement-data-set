"""
reading and merging the data for a single report.
"""

import re
from dataclasses import dataclass
from io import StringIO
from typing import Optional, List, Dict

import numpy as np
import pandas as pd

from secfsdstools._0_config.configmgt import Configuration, ConfigurationManager
from secfsdstools._0_utils.fileutils import read_content_from_file_in_zip
from secfsdstools._3_index.indexdataaccess import IndexReport, DBIndexingAccessor

NUM_TXT = "num.txt"
PRE_TXT = "pre.txt"

NUM_COLS = ['adsh', 'tag', 'version', 'coreg', 'ddate', 'qtrs', 'uom', 'value', 'footnote']
PRE_COLS = ['adsh', 'report', 'line', 'stmt', 'inpth', 'rfile',
            'tag', 'version', 'plabel', 'negating']


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


def match_group_iter(match_iter):
    """
    returns an iterator that returns the group() of the matching iterator
    :param match_iter:
    :return: group content iterator
    """
    for match in match_iter:
        yield match.group()


class ReportReader:
    """
    reading the data for a single report. also provides several convenient methods
    to prepare and aggregate the raw data
    """

    @classmethod
    def get_report_by_adsh(cls, adsh: str, configuration: Optional[Configuration] = None):
        """
        creates the ReportReader instance for a certain adsh.
        if no configuration is passed, it reads the config from the config file
        :param ads: ads
        :param configuration: Optional configuration object
        :return: instance of Company Reader
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = DBIndexingAccessor(db_dir=configuration.db_dir)
        return ReportReader(dbaccessor.read_index_report_for_adsh(adsh=adsh))

    def __init__(self, report: IndexReport):
        self.report = report
        self.num_df: Optional[pd.DataFrame]
        self.pre_df: Optional[pd.DataFrame]

        self.adsh_pattern = re.compile(f'^{report.adsh}.*$', re.MULTILINE)

    def _read_df_from_raw(self, file_in_zip: str, column_names: List[str]) \
            -> pd.DataFrame:
        """
        reads the num.txt or pre.txt directly from the zip file into a df.
        uses re to first filter only the rows that belong to the report
        and only then actually create the df
        """
        content = read_content_from_file_in_zip(self.report.fullPath, file_in_zip)
        lines = "\n".join(match_group_iter(self.adsh_pattern.finditer(content)))
        return pd.read_csv(StringIO(lines), sep="\t", header=None, names=column_names)

    def _read_raw_data(self):
        """
        read the raw data from the num and pre file into dataframes and store them inside the object
        :return:
        """
        self.num_df = self._read_df_from_raw(NUM_TXT, NUM_COLS)
        self.pre_df = self._read_df_from_raw(PRE_TXT, PRE_COLS)

    def get_raw_num_data(self) -> pd.DataFrame:
        """
        returns a copy of the raw dataframe for the num.txt file of this report
        :return: pd.DataFrame
        """
        return self.num_df.copy()

    def get_raw_pre_data(self) -> pd.DataFrame:
        """
        returns a copy of the raw dataframe for the pre.txt file of this report
        :return: pd.DataFrame
        """
        return self.pre_df.copy()

    def financial_statements_for_dates_and_tags(self,
                                                dates: Optional[List[int]] = None,
                                                tags: Optional[List[str]] = None,
                                                ) -> pd.DataFrame:
        """
        creates the financial statements dataset by merging the pre and num
         sets together. It also filters out only the ddates that are
         inside the list.
        Note: the dates are int in the form YYYYMMDD
        :param dates: list with ddates to filter for
        :param tags: list with tags to consider
        :return: pd.DataFrame
        """

        num_df_filtered_for_dates = self.num_df
        if dates:
            num_df_filtered_for_dates = self.num_df[self.num_df.ddate.isin(dates)]

        pre_filtered_for_tags = self.pre_df
        if tags:
            pre_filtered_for_tags = self.pre_df[self.pre_df.tag.isin(tags)]

        num_pre_merged_df = pd.merge(num_df_filtered_for_dates,
                                     pre_filtered_for_tags,
                                     on=['adsh', 'tag', 'version'])
        num_pre_merged_pivot_df = num_pre_merged_df.pivot_table(
            index=['adsh', 'tag', 'version', 'stmt', 'report', 'line', 'uom', 'negating', 'inpth'],
            columns='ddate',
            values='value')
        num_pre_merged_pivot_df.rename_axis(None, axis=1, inplace=True)
        num_pre_merged_pivot_df.sort_values(['stmt', 'report', 'line', 'inpth'], inplace=True)
        num_pre_merged_pivot_df.reset_index(drop=False, inplace=True)
        return num_pre_merged_pivot_df

    def financial_statements_for_period(self, tags: Optional[List[str]] = None, ) -> pd.DataFrame:
        """
        returns the merged and pivoted table for the of the num-
         and predata for the current date only
        :param tags: List with tags to include or None
        :return:
        """
        return self.financial_statements_for_dates_and_tags(dates=[self.report.period], tags=tags)

    def financial_statements_for_period_and_previous_period(
            self, tags: Optional[List[str]] = None, ) -> pd.DataFrame:
        """
        returns the merged and pivoted table for the of the num-
         and predata for the current and the date
         of the same period a year ago.
        :param tags: List with tags to include or None
        :return: pd.DataFrame
        """

        # note: -10_000 selects the last year, since int values are used for ddate
        return self.financial_statements_for_dates_and_tags(
            dates=[self.report.period, self.report.period - 10_000], tags=tags)

    def statistics(self) -> BasicReportStats:
        """
        calculate a few simple statistics of a report.
        - number of entries in the num-file
        - number of entries in the pre-file
        - number of facts per ddate (in num file)
        - list of different statements in the pre file
        - list of tags per statement

        :return: BasicReportsStats instance
        """
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
