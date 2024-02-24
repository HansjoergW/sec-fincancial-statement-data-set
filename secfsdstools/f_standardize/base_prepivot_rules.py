"""
Contains PrePivotRules Definitions.
"""
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


class PrePivotCorrectSign(PrePivotRule):
    """
    Certain tags are expected to be either positive or negative when present.
    (e.g. assets in the BS are always shown as a positive number).

    However, sometimes these values are mixed up, also when the "negating" flag is taken into
    consideration.

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
        self.tag_list = tag_list
        self.is_positive = is_positive

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns
        """
        return self.tag_list

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        return set(self.tag_list)

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            data_df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        return (data_df[self.mixed_up_summand] ==
                data_df[self.sum_tag] + data_df[self.other_summand]) \
            & (data_df[self.other_summand] > 0)

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """

        mixed_up_values = data_df[self.mixed_up_summand].copy()
        data_df.loc[mask, self.mixed_up_summand] = data_df[self.sum_tag]
        data_df.loc[mask, self.sum_tag] = mixed_up_values

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        pos_neg_text = "positive" if self.is_positive else "negative"

        return f"Ensures that the tags {self.tag_list} have a {pos_neg_text} value. Applied when " \
               f"the expectation of having a negative or positive value is not met"