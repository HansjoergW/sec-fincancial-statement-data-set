"""
Contains PrePivotRules Definitions.
"""
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

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> pd.DataFrame:
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """
        data_df.drop(data_df[mask].index, inplace=True)
        return data_df

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Deduplicates the dataframe based on the columns {self.index_cols + ['value']}"


class PrePivotCorrectSign(PrePivotRule):
    """
    Certain tags are expected to be either positive or negative when present, resp. should
    in all reports be displayed the same way:
    (e.g. Assets in the BS is always shown as a positive number)

    However, sometimes these values are mixed up, also when considering the "negating" flag.

    E.g. CostOfGoodAndServices is displayed positive in 95% of the cases and
     in 5% of the cases it is displayed "positive" but with the negating flag set.

    This rules ensures, that the provided tags have the expected sign.
    """

    def __init__(self, tag_list: List[str], is_positive: bool):
        """
        ensure that the values of the tags in the tag_list are positive (if is_positive is true),
        or are negative (if is_positive is false).

        Args:
            tag_list: list with the names of tag that have to be checked
            is_positive: flag that indicates whether positive or negative values are expected
        """
        super().__init__("CorSign")
        self.tag_list = tag_list
        self.is_positive = is_positive

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            data_df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        return data_df.tag.isin(self.tag_list) & ((data_df.value < 0) == self.is_positive)

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> pd.DataFrame:
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """
        data_df.loc[mask, 'value'] = data_df.value * -1
        return data_df

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        pos_neg_text = "positive" if self.is_positive else "negative"

        return f"Ensures that the tags {self.tag_list} have a {pos_neg_text} value. Applied when " \
               f"the expectation of having a negative or positive value is not met"
