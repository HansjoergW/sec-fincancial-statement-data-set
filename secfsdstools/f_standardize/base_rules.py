""" This module contains the Base Rule implementations."""
from typing import Set, List

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_rule_framework import Rule, RuleEntity


class PreSumUpCorrection(Rule):
    """
    it happens, that the values for Assets and AssetsNoncurrent
    were mixed up, resp. swapped when tagged:  Example is adsh 0001692981-19-000022

    So instead of Assets = AssetsCurrent + AssetsNoncurrent
    it matches AssetsNoncurrent = Assets + AssetsCurrent.

    That is obviously wrong, so this rule corrects such a constellation.

    This rule is usually used in a preprocess step.
    """

    def __init__(self, sum_tag: str, mixed_up_summand: str, other_summand: str):
        """

        Args:
            sum_tag: the tag that should contain the sum
            mixed_up_summand: the summand that actually does contain the sum
            other_summand: the other summand
        """
        self.sum_tag = sum_tag
        self.mixed_up_summand = mixed_up_summand
        self.other_summand = other_summand

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns
        """
        return [self.sum_tag, self.mixed_up_summand]

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        return {self.sum_tag, self.mixed_up_summand, self.other_summand}

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        return (df[self.mixed_up_summand] == df[self.sum_tag] + df[self.other_summand]) \
               & (df[self.other_summand] > 0)

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """

        mixed_up_values = df[self.mixed_up_summand].copy()
        df.loc[mask, self.mixed_up_summand] = df[self.sum_tag]
        df.loc[mask, self.sum_tag] = mixed_up_values

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Swaps the values between the tag '{self.sum_tag}' and '{self.mixed_up_summand}' " \
               f"if the following equation is True " \
               f"\"'{self.mixed_up_summand}' = '{self.sum_tag}' + '{self.other_summand}\" " \
               f" and '{self.other_summand}' > 0"


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

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

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

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

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


class MissingSummandRule(Rule):
    """ calculates the value of a missing summand if all other summands are and the sum is set.
        """

    def __init__(self, sum_tag: str, missing_summand_tag: str, existing_summands_tags: List[str]):
        """
        Args:
            sum_tag: the tag that contains the sum
            missing_summand_tag: the summand tag that is not set
            existing_summands_tags: the summand tags which have values
        """
        self.sum_tag = sum_tag
        self.missing_summand_tag = missing_summand_tag
        self.existing_summands_tags = existing_summands_tags

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns

        """
        return [self.missing_summand_tag]

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        result = {self.sum_tag, self.missing_summand_tag}
        result.update(self.existing_summands_tags)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """

        # sum and other_summands must be set, missing_summand must not be set
        mask = ~df[self.sum_tag].isna()
        for summand_name in self.existing_summands_tags:
            mask = mask & ~df[summand_name].isna()
        mask = mask & df[self.missing_summand_tag].isna()
        return mask

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """

        df.loc[mask, self.missing_summand_tag] = \
            df[self.sum_tag] - df[self.existing_summands_tags].sum(axis=1)

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Calculates the value for the missing column '{self.missing_summand_tag}' by " \
               f"subtracting the values of the columns '{self.existing_summands_tags}' from the " \
               f"column '{self.sum_tag}' if all of the columns " \
               f"{[self.sum_tag, *self.existing_summands_tags]} are set."


class SumUpRule(Rule):
    """Sums app the available Summands to a target column if the target colum is not set"""

    def __init__(self, sum_tag: str, potential_summands: List[str]):
        """
        Args:
            sum_tag: the tag containing the sum
            potential_summands: potential tags with values that have to be summed up
        """
        self.sum_tag = sum_tag
        self.potential_summands = potential_summands

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
        result.update(self.potential_summands)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """

        # mask if the target was not set
        return df[self.sum_tag].isna()

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """
        df.loc[mask, self.sum_tag] = 0.0  # initialize
        for potential_summand in self.potential_summands:
            summand_mask = mask & ~df[potential_summand].isna()
            df.loc[summand_mask, self.sum_tag] = df[self.sum_tag] + df[potential_summand]

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Sums up the availalbe values in the columns {self.potential_summands} into" \
               f" the column '{self.sum_tag}', if the column '{self.sum_tag}' is nan"


class SetSumIfOnlyOneSummand(Rule):
    """
    If the sumt_tag is nan and only one summand has a value, then the
    sum_tag gets the copy of the summand available and the other summands are set to zero.
    """

    def __init__(self, sum_tag: str, summand_set: str, summands_nan: List[str]):
        """
        
        Args:
            sum_tag: target that contains the sum. 
            summand_set: the summand that contains a value
            summands_nan: the summands without a value
        """
        self.sum_tag = sum_tag
        self.summand_set = summand_set
        self.summands_nan = summands_nan

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns
        """
        return [self.sum_tag, *self.summands_nan]

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        result = {self.sum_tag, self.summand_set}
        result.update(self.summands_nan)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        mask = df[self.sum_tag].isna() & ~df[self.summand_set].isna()
        for summand_nan in self.summands_nan:
            mask = mask & df[summand_nan].isna()

        return mask

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """
        df.loc[mask, self.sum_tag] = df[self.summand_set]  # initialize
        for summand_nan in self.summands_nan:
            df.loc[mask, summand_nan] = 0.0

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Copies the value of the column '{self.summand_set}' into the column " \
               f"'{self.sum_tag}' and sets the columns {self.summands_nan} to 0.0 if the " \
               f"column '{self.summand_set} is set and the columns " \
               f"{[self.sum_tag, *self.summands_nan]} are nan."


class PostCopyToFirstSummand(Rule):
    """
    if the sum_tag is set and the first summand and the other summands are nan,
    then copy the sum_tag value to the first summand and set the other summands to '0.0'
    """

    def __init__(self, sum_tag: str, first_summand: str, other_summands: List[str]):
        """

        Args:
            sum_tag: set sum_tag
            first_summand: missing first summand
            other_summands: missing second summand
        """
        self.sum_tag = sum_tag
        self.first_summand = first_summand
        self.other_summands = other_summands

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns
        """
        return [self.first_summand, *self.other_summands]

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        result = {self.sum_tag, self.first_summand}
        result.update(self.other_summands)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        mask = ~df[self.sum_tag].isna() & df[self.first_summand].isna()
        for other_summand in self.other_summands:
            mask = mask & df[other_summand].isna()

        return mask

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """

        df.loc[mask, self.first_summand] = df[self.sum_tag]  # initialize
        for other_summand in self.other_summands:
            df.loc[mask, other_summand] = 0.0

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Copies the value of the '{self.sum_tag}' to the first summand " \
               f"'{self.first_summand}' and set the other summands {self.other_summands} to 0.0 " \
               f"if '{self.sum_tag} is set and the summands " \
               f"{[self.first_summand, *self.other_summands]} are nan."


def missingsumparts_rules_creator(sum_tag: str, summand_tags: List[str]) -> List[RuleEntity]:
    """
    Helper method that
    creates a list with rules that handle all the cases if one element of
    an addition is not set. For instance, called with 'Assets' and
    [AssetsCurrent, AssetsNoncurrent] it would create three rules:

    - MissingSumRule(Assets, [AssetsCurrent, AssetsNoncurrent])
    - MissingSummandRule(Assets, AssetsCurrent, AssetsNoncurrent)
    - MissingSummandRule(Assets, AssetsNoncurrent, AssetsCurrent)

    Args:
        sum_tag: name of the column that contains the sum
        summand_tags: name of the summands

    Returns:
        List[RuleEntity]
    """

    rules: List[RuleEntity] = [MissingSumRule(sum_tag=sum_tag,
                                              summand_tags=summand_tags)]
    for summand in summand_tags:
        others = summand_tags.copy()
        others.remove(summand)
        rules.append(MissingSummandRule(sum_tag=sum_tag,
                                        missing_summand_tag=summand,
                                        existing_summands_tags=others))
    return rules


def setsumifonlyonesummand_rules_creator(sum_tag: str, summand_tags: List[str]) -> List[RuleEntity]:
    """
    Helper method that creates all the rules to copy the value from one summand to the sum, if
    all other summands are not set.

    Args:
        sum_tag: tag that contains the sum
        summand_tags:  all summand tags

    Returns:
        List[RuleEntity]: List of SetSumIfOnlyOneSummand instances
    """

    rules: List[RuleEntity] = []

    for summand in summand_tags:
        others = summand_tags.copy()
        others.remove(summand)
        rules.append(SetSumIfOnlyOneSummand(sum_tag=sum_tag,
                                            summand_set=summand,
                                            summands_nan=others))
    return rules
