"""
base logic for reporting classes
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict

import pandas as pd

NUM_TXT = "num.txt"
PRE_TXT = "pre.txt"
SUB_TXT = "sub.txt"

NUM_COLS = ['adsh', 'tag', 'version', 'coreg', 'ddate', 'qtrs', 'uom', 'value', 'footnote']
PRE_COLS = ['adsh', 'report', 'line', 'stmt', 'inpth', 'rfile',
            'tag', 'version', 'plabel', 'negating']

SUB_COLS = ['adsh', 'form', 'period', 'filed', 'cik']


def match_group_iter(match_iter):
    """
    returns an iterator that returns the group() of the matching iterator
    Args:
        match_iter: a re match iterator

    Returns:
        group content iterator
    """
    for match in match_iter:
        yield match.group()


class BaseReportReader(ABC):
    """
    BaseReportReader
    """

    def __init__(self):
        self.num_df: Optional[pd.DataFrame] = None
        self.pre_df: Optional[pd.DataFrame] = None
        self.sub_df: Optional[pd.DataFrame] = None
        self.adsh_form_map: Optional[Dict[str, int]] = None
        self.adsh_period_map: Optional[Dict[str, int]] = None
        self.adsh_previous_map: Optional[Dict[str, int]] = {}

    @staticmethod
    def _calculate_previous_period(period: int) -> int:
        previous_value = period - 10_000
        period_monthday = period % 10_000
        period_year = period // 10_000

        if period % 10_000 == 229:
            previous_value = previous_value - 1

        # was the previous year a leap year and is the period for end of february
        if (((period_year - 1) % 4) == 0) & (period_monthday == 228):
            previous_value = previous_value + 1

        return previous_value

    def _read_raw_data(self):
        """
        read the raw data from the num and pre file into dataframes and store them
        inside the object. used in a lazy loading manner.
        """
        if self.num_df is None:
            self.num_df = self._read_df_from_raw(file_type=NUM_TXT)
            # pandas pivot works better if coreg is not nan, so we set it here to a simple dash
            self.num_df.loc[self.num_df.coreg.isna(), 'coreg'] = '-'
            self.pre_df = self._read_df_from_raw(file_type=PRE_TXT)
            self.sub_df = self._read_df_from_raw(file_type=SUB_TXT)
            self.adsh_form_map = \
                self.sub_df[['adsh', 'form']].set_index('adsh').to_dict()['form']
            self.adsh_period_map = \
                self.sub_df[['adsh', 'period']].set_index('adsh').to_dict()['period']

            # caculate the date for the previous year
            self.adsh_previous_map = {adsh: BaseReportReader._calculate_previous_period(period)
                                      for adsh, period in self.adsh_period_map.items()}

    def get_raw_num_data(self) -> pd.DataFrame:
        """
        returns a copy of the raw dataframe for the num.txt file of this report

        Returns:
            pd.DataFrame: pandas dataframe
        """
        self._read_raw_data()  # lazy load the data if necessary
        return self.num_df.copy()

    def get_raw_pre_data(self) -> pd.DataFrame:
        """
        returns a copy of the raw dataframe for the pre.txt file of this report

        Returns:
            pd.DataFrame: pandas dataframe
        """
        self._read_raw_data()  # lazy load the data if necessary
        return self.pre_df.copy()

    def get_raw_sub_data(self) -> pd.DataFrame:
        """
        returns a copy of the raw dataframe for the sub.txt file of this report

        Returns:
            pd.DataFrame: pandas dataframe
        """
        self._read_raw_data()  # lazy load the data if necessary
        return self.sub_df.copy()

    def merge_pre_and_num(self,
                          use_period: bool = True,
                          use_previous_period: bool = False,
                          tags: Optional[List[str]] = None,
                          ) -> pd.DataFrame:
        """
        merges the raw data of pre and num together.
        depending on the parameters, it just uses the  period date and the previouis period date.
        furthermore, also the tags could be restricted.

        Note: default for use_period is True

        Args:
            use_period (bool, True): indicates that only the values are filtered which
            ddates machtes the period of the report.

            use_previous_period (bool, False): indicates that only the values  are filtered
            which ddates matches the period of the report and the previous year. If this is set
            to True, then the value of use_period is ignored

            tags (List[str], optional, None): if set, only the tags listet in this
            parameter are returned
        Returns:
            pd.DataFrame: pandas dataframe

        """
        self._read_raw_data()  # lazy load the data if necessary
        num_df_filtered_for_dates = self.num_df

        ## only consider the entries in num.df that have ddates according to the set
        ## use_report and use_previoius_report

        # if use_previous_report, then use_period is ignored
        if use_period and not use_previous_period:
            mask = self.num_df['adsh'].map(self.adsh_period_map) == self.num_df['ddate']
            num_df_filtered_for_dates = num_df_filtered_for_dates[mask]

        if use_previous_period:
            mask = (self.num_df['adsh'].map(self.adsh_period_map) == self.num_df['ddate']) | \
                   (self.num_df['adsh'].map(self.adsh_previous_map) == self.num_df['ddate'])

            num_df_filtered_for_dates = num_df_filtered_for_dates[mask]

        ## only consider the entries in pre which tag names are defined in the tags parameter
        pre_filtered_for_tags = self.pre_df
        if tags:
            pre_filtered_for_tags = self.pre_df[self.pre_df.tag.isin(tags)]

        ## transform the data
        # merge num and pre together. only rows in num are considered for which entries in pre exist
        num_pre_merged_df = pd.merge(num_df_filtered_for_dates,
                                     pre_filtered_for_tags,
                                     on=['adsh', 'tag', 'version'])

        return num_pre_merged_df

    def financial_statements_for_tags(self,
                                      use_period: bool = True,
                                      use_previous_period: bool = False,
                                      tags: Optional[List[str]] = None,
                                      ) -> pd.DataFrame:
        """
        formats the raw data in a way, that it reflects the presentation of the
        primary financial statements (balance sheet, income statement, cash flow)
        of the original filed report. Meaning the statements are grouped per report
        and per type and have the same order as they appear in the report itself.
        moreover, the data is pivoted, so that every ddate has its own column

        Args:
            use_period (bool, True): indicates that only the values are filtered which
            ddates machtes the period of the report.

            use_previous_period (bool, False): indicates that only the values  are filtered
            which ddates matches the period of the report and the previous year. If this is set
            to True, then the value of use_period is ignored

            tags (List[str], optional, None): if set, only the tags listet in this
            parameter are returned

        Returns:
            pd.DataFrame: the filtered and transformed data
        """

        ## transform the data
        # merge num and pre together. only rows in num are considered for which entries in pre exist
        num_pre_merged_df = self.merge_pre_and_num(use_period=use_period,
                                                   use_previous_period=use_previous_period,
                                                   tags=tags)

        # pivot the data, so that ddate appears as a column
        num_pre_merged_pivot_df = num_pre_merged_df.pivot_table(
            index=['adsh', 'coreg', 'tag', 'version', 'stmt',
                   'report', 'line', 'uom', 'negating', 'inpth'],
            columns='ddate',
            values='value')

        # some cleanup and ordering
        num_pre_merged_pivot_df.rename_axis(None, axis=1, inplace=True)
        num_pre_merged_pivot_df.sort_values(['adsh', 'coreg', 'stmt', 'report', 'line', 'inpth'],
                                            inplace=True)
        num_pre_merged_pivot_df.reset_index(drop=False, inplace=True)

        # adding the report type as an additional column
        num_pre_merged_pivot_df['form'] = num_pre_merged_pivot_df['adsh'].map(self.adsh_form_map)

        # the values for ddate are ints, not string
        # if we pivot, then the column names stay ints, which is unuexpected, so we change the
        # the type of the column to strings
        num_pre_merged_pivot_df.rename(columns={x: str(x) for x in num_pre_merged_pivot_df.columns},
                                       inplace=True)

        return num_pre_merged_pivot_df

    def financial_statements_for_period(self, tags: Optional[List[str]] = None, ) -> pd.DataFrame:
        """
        returns the merged and pivoted table for the of the num-
         and predata for the current date only
        Args:
            tags (List[str], optional): List with tags to include or None

        Returns:
            pd.Dataframe: pandas dataframe
        """
        return self.financial_statements_for_tags(use_period=True, tags=tags)

    def financial_statements_for_period_and_previous_period(
            self, tags: Optional[List[str]] = None, ) -> pd.DataFrame:
        """
        returns the merged and pivoted table for the of the num-
         and predata for the current and the date
         of the same period a year ago.

        Args:
            tags (List[str], optional): List with tags to include or None

        Returns:
            pd.Dataframe: pandas dataframe
        """

        return self.financial_statements_for_tags(use_previous_period=True, tags=tags)

    @abstractmethod
    def _read_df_from_raw(self,
                          file_type: str) \
            -> pd.DataFrame:
        """

        Args:
            file_type: SUB_TXT, PRE_TXT, or NUM_TXT

        Returns:
            pd.DataFrame: the content for the read filetype
        """
