"""
base logic for reporting classes
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict

import pandas as pd

from secfsdstools.a_utils.basic import calculate_previous_period
from secfsdstools.a_utils.constants import NUM_TXT, PRE_TXT, SUB_TXT


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
