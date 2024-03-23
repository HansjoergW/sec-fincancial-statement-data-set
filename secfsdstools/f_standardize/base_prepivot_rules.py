from typing import List

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_rule_framework import PrePivotRule


class PrePivotDeduplicate(PrePivotRule):
    """
    Deduplicates the dataset based on the index_cols that are defined in the base class.

    sometimes, only single tags are duplicated, however, there are also reports
    where all tags of a report are duplicated.

    without deduplication, the pivot command will fail.
    """

    def __init__(self):
        super().__init__("DeDup")

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            data_df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        return data_df.duplicated(self.index_cols)

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """
        data_df.drop(data_df[mask].index, inplace=True)

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Deduplicates the dataframe based on the columns {self.index_cols + ['value']}"
