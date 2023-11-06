""" This module contains the Base Rule implementations."""
from typing import Set, List

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.rule_framework import Rule


class CopyTagRule(Rule):
    """ Copies the content of the original tag into the target tag, if the target tag is not set
    and the original tag is set"""

    def __init__(self, original: str, target: str):
        """
        Args:
            original: the name of the tag from which the value is copied
            target: the name of the tag to which the value is copied
        """
        self.original = original
        self.target = target

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        return (df[self.target].isna() &
                ~df[self.original].isna())

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so now new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """
        df.loc[mask, self.target] = df[self.original]

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        return {self.target, self.original}

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns

        """
        return [self.target]

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Copies the values from {self.original} to {self.target} " \
               f"if {self.original} is not null and {self.target} is null"


class MissingSumRule(Rule):
    """ sums up the value from the summand_tags columns into the
        sum_tag if the sum_tag is nan and all summand_tags have values"""

    def __init__(self, sum_tag: str, summand_tags: List[str]):
        """
        Args:
            sum_tag: tag which shall contain the sum of the summand_tags
            summand_tags: tags which values have to be summed up
        """
        self.sum_tag = sum_tag
        self.summand_tags = summand_tags

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns

        """
        return [self.sum_tag]

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        result = {self.sum_tag}
        result.update(self.summand_tags)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        mask = df[self.sum_tag].isna()
        for summand_name in self.summand_tags:
            mask = mask & ~df[summand_name].isna()

        return mask

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so now new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """
        df.loc[mask, self.sum_tag] = df[self.summand_tags].sum(axis=1)

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Sums up the values in the columns {self.summand_tags} into the column " \
               f"'{self.sum_tag}', if the column '{self.sum_tag}' is nan and if all columns" \
               f" {self.summand_tags} have a value"