"""
This module defines a simple rule framework that allows to define concrete rules and build
a rule hierarchy that then is used in the standardizer.
"""
from abc import ABC
from abc import abstractmethod
from typing import Optional, Set, List

import pandas as pd
import pandera as pa


class RuleEntity(ABC):
    """ Base RuleEntity, which provides the basic definition to craete a tree of rules. """
    id: str = '<not set>'

    @abstractmethod
    def get_input_tags(self) -> Set[str]:
        """
        Returns the List of tags that used by this rule entity. All these tags have to be
        present as columns in the dataframe that is provided in the process method.

        Returns:
            Set[str] : a set of strings
        """
        pass

    def process(self, df: pd.DataFrame, log_df: Optional[pd.DataFrame] = None):
        """
        Applies the rules on the provided dataframe.
        If a log_df is provided, the affected roles by every rule are logged into the
        log_df.

        Args:
            df (pd.DataFrame): the dataframe on which the rules will be applied
            log_df (pd.DataFrame, optional, None): the logs-dataframe

        """
        pass

    @abstractmethod
    def set_id(self, prefix: str):
        """
        sets the identifier of this rule entity
        Args:
            prefix: the prefix from the parent in the rule-tree which has to be part of the id
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        pass


class Rule(RuleEntity):
    """Base class to define a single rule"""

    @abstractmethod
    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns

        """
        pass

    def get_target_tags_str(self) -> str:
        """
        returns a single string in which all target_tags are separated by a "/"
        Returns:
            str: a string containing all target tags
        """
        return "/".join(self.get_target_tags())

    @abstractmethod
    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        pass

    @abstractmethod
    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so now new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """
        pass

    def set_id(self, prefix: str):
        """
        sets the identifier of this rule entity
        Args:
            prefix: the prefix from the parent in the rule-tree which has to be part of the id
        """
        self.id = f'{prefix}_{self.get_target_tags_str()}'

    def process(self, df: pd.DataFrame, log_df: Optional[pd.DataFrame] = None):
        """
        process the dataframe and apply the rule. If a log_df is provided, the rows on which
        the rule is applied to is added to the log_df.

        Args:
            df (pd.DataFrame) : dataframe on which the rule has to be applied
            log_df (pd.DataFrame, optional, None): the log dataframe
        """
        mask = self.mask(df)
        self.apply(df, mask)

        # if a log_df is provided, create a new column for this role and mark the rows that were
        # affected by the rule
        if (log_df is not None) and (len(log_df) == len(mask)):
            log_df[self.id] = False
            log_df.loc[mask, self.id] = True


class RuleGroup(RuleEntity):
    """Class which can manage a group of rule entities"""

    def __init__(self, rules: List[RuleEntity], prefix: str, description: str = ""):
        """
        Args:
            rules (List[RuleEntity]]): a list of RuleEntitys
            prefix: identification prefix
            description: a short description of the purpose of the group
        """
        self.rules = rules
        self.prefix = prefix
        self.description = description

    def set_id(self, parent_prefix: str):
        """
        sets the identifier of this rulegroup and calls the set_id method of the child rules
        Args:
            prefix: the prefix from the parent in the rule-tree which has to be part of the id
        """
        idx = 1
        self.id = f'{parent_prefix}_{self.prefix}'
        for rule in self.rules:
            rule.set_id(f'{self.id}_#{idx}')
            idx = idx + 1

    def process(self, df: pd.DataFrame, log_df: Optional[pd.DataFrame] = None):
        """
        process the dataframe and apply the rules of this group.
        If a log_df is provided, the rows on which the rule is applied to is added to the log_df.

        Args:
            df (pd.DataFrame) : dataframe on which the rule has to be applied
            log_df (pd.DataFrame, optional, None): the log dataframe
        """
        for rule in self.rules:
            rule.process(df=df, log_df=log_df)

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        result: Set[str] = set()
        for rule in self.rules:
            result.update(rule.get_input_tags())
        return result

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """

        return self.description