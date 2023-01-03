"""
reading and merging the data for a single report.
"""

import os.path
import re
from io import StringIO
from typing import Optional, List

import pandas as pd

from secfsdstools._0_utils.fileutils import read_content_from_file_in_zip
from secfsdstools._3_index.indexdataaccess import IndexReport

NUM_TXT = "num.txt"
PRE_TXT = "pre.txt"

NUM_COLS = ['adsh', 'tag', 'version', 'coreg', 'ddate', 'qtrs', 'uom', 'value', 'footnote']
PRE_COLS = ['adsh', 'report', 'line', 'stmt', 'inpth', 'rfile',
            'tag', 'version', 'plabel', 'negating']


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

    def __init__(self, report: IndexReport, zip_dir: str):
        self.report = report
        self.zip_file_path = os.path.join(zip_dir, report.originFile)
        self.num_df: Optional[pd.DataFrame]
        self.pre_df: Optional[pd.DataFrame]

        self.adsh_pattern = re.compile(f"^{report.adsh}.*$", re.MULTILINE)

    def _read_df_from_raw(self, file_in_zip: str, column_names: List[str]) \
            -> pd.DataFrame:
        """
        reads the num.txt or pre.txt directly from the zip file into a df.
        uses re to first filter only the rows that belong to the report
        and only then actually create the df
        """
        content = read_content_from_file_in_zip(self.zip_file_path, file_in_zip)
        lines = "\n".join(match_group_iter(self.adsh_pattern.finditer(content)))
        return pd.read_csv(StringIO(lines), sep="\t", header=None, names=column_names)

    def _read_raw_data(self):
        """
        read the raw data from the num and pre file into dataframes and store them inside the object
        :return:
        """
        self.num_df = self._read_df_from_raw(NUM_TXT, NUM_COLS)
        self.pre_df = self._read_df_from_raw(PRE_TXT, PRE_COLS)

    def _financial_statements_for_dates(self, dates: List[int]) -> pd.DataFrame:
        num_df_filtered_for_dates = self.num_df[self.num_df.ddate.isin(dates)]
        num_pre_merged_df = pd.merge(num_df_filtered_for_dates,
                                     self.pre_df,
                                     on=['adsh', 'tag', 'version'])
        num_pre_merged_pivot_df = num_pre_merged_df.pivot_table(
            index=["adsh", "tag", "version", "stmt", "report", "line", "uom", "negating", "inpth"],
            columns="ddate",
            values="value")
        num_pre_merged_pivot_df.rename_axis(None, axis=1, inplace=True)
        num_pre_merged_pivot_df.sort_values(['stmt', 'report', 'line', 'inpth'], inplace=True)
        num_pre_merged_pivot_df.reset_index(drop=False, inplace=True)
        return num_pre_merged_pivot_df
