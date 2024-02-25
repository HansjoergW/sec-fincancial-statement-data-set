"""
This module defines a simple rule framework that allows to define concrete rules and build
a rule hierarchy that then is used in the standardizer.
"""
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional, Set, List

import pandas as pd
import pandera as pa


@dataclass
class DescriptionEntry:
    """ Dataclass used to collect the rule descriptions and to create the
    process descrption table. """
    part: str
    type: str
    ruleclass: str
    identifier: str
    description: str


class RuleEntity(ABC):
    """ Base RuleEntity, which provides the basic definition to craete a tree of rules. """
    identifier: str = '<not set>'

    @abstractmethod
    def get_input_tags(self) -> Set[str]:
        """
        Returns the List of tags that used by this rule entity. All these tags have to be
        present as columns in the dataframe that is provided in the process method.

        Returns:
            Set[str] : a set of strings
        """

    def process(self, data_df: pd.DataFrame, log_df: Optional[pd.DataFrame] = None):
        """
        Applies the rules on the provided dataframe.
        If a log_df is provided, the affected roles by every rule are logged into the
        log_df.

        Args:
            data_df (pd.DataFrame): the dataframe on which the rules will be applied
            log_df (pd.DataFrame, optional, None): the logs-dataframe

        """

    @abstractmethod
    def set_id(self, prefix: str):
        """
        sets the identifier of this rule entity
        Args:
            prefix: the prefix from the parent in the rule-tree which has to be part of the id
        """

    @abstractmethod
    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """

    @abstractmethod
    def collect_description(self, part: str) -> List[DescriptionEntry]:
        """
        Returns the description of these elements and its children as a list

        Args:
            part (str): the part to which the element belongs to
        Returns:
            List (DescriptionEntry)
        """


class AbstractRule(RuleEntity):
    """
    Abstract Base class für single rules
    """

    @abstractmethod
    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            data_df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """

    @abstractmethod
    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so now new dataframe is produced.

        Args:
            data_df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """

    def process(self, data_df: pd.DataFrame, log_df: Optional[pd.DataFrame] = None):
        """
        process the dataframe and apply the rule. If a log_df is provided, the rows on which
        the rule is applied to is added to the log_df.

        Args:
            data_df (pd.DataFrame) : dataframe on which the rule has to be applied
            log_df (pd.DataFrame, optional, None): the log dataframe
        """
        mask = self.mask(data_df)
        self.apply(data_df, mask)

        # if a log_df is provided, create a new column for this role and mark the rows that were
        # affected by the rule
        if (log_df is not None) and (len(log_df) == len(mask)):
            log_df[self.identifier] = False
            log_df.loc[mask, self.identifier] = True

    def collect_description(self, part: str) -> List[DescriptionEntry]:
        """
        Returns the description of this rule elements and its children as a list.

        Args:
            part (str): the part to which the element belongs to

        Returns:
            List (DescriptionEntry)
        """
        return [DescriptionEntry(
            part=part,
            type="Rule",
            ruleclass=self.__class__.__name__,
            identifier=self.identifier,
            description=self.get_description())]


class PrePivotRule(AbstractRule):
    index_cols = ['adsh', 'coreg', 'report', 'uom', 'tag', 'version', 'ddate']

    """Base class to define a single rule that is applied before the dataframe was pivoted"""

    def __init__(self, rule_id: str):
        self.rule_id = rule_id

    def set_id(self, prefix: str):
        """
        sets the identifier of this rule entity
        Args:
            prefix: the prefix from the parent in the rule-tree which has to be part of the id
        """
        self.identifier = f'{prefix}_{self.rule_id}'

    def process(self, data_df: pd.DataFrame, log_df: Optional[pd.DataFrame] = None):
        """
        process the dataframe and apply the rule. If a log_df is provided, the rows on which
        the rule is applied to is added to the log_df.

        Args:
            data_df (pd.DataFrame) : dataframe on which the rule has to be applied
            log_df (pd.DataFrame, optional, None): the log dataframe
        """
        mask = self.mask(data_df)

        # if a log_df is provided, create a new column for this role and mark the rows that were
        # affected by the rule
        if log_df is not None:
            # since apply can also change the data_df, we have to create the log changes, before
            # the rule is applied
            affected_rows_df = data_df[mask][self.index_cols].copy()
            affected_rows_df['id'] = self.identifier
            log_df.append(affected_rows_df, ignore_index=True)

        self.apply(data_df, mask)

    def get_input_tags(self) -> Set[str]:
        return set()  # not relevant for PrePivotRules


class Rule(AbstractRule):
    """Base class to define a single rule that is used after the dataframe was pivoted"""

    @abstractmethod
    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns
        """

    def get_target_tags_str(self) -> str:
        """
        returns a single string in which all target_tags are separated by a "/"
        Returns:
            str: a string containing all target tags
        """
        return "/".join(self.get_target_tags())

    def set_id(self, prefix: str):
        """
        sets the identifier of this rule entity
        Args:
            prefix: the prefix from the parent in the rule-tree which has to be part of the id
        """
        self.identifier = f'{prefix}_{self.get_target_tags_str()}'

    def process(self, data_df: pd.DataFrame, log_df: Optional[pd.DataFrame] = None):
        """
        process the dataframe and apply the rule. If a log_df is provided, the rows on which
        the rule is applied to is added to the log_df.

        Args:
            data_df (pd.DataFrame) : dataframe on which the rule has to be applied
            log_df (pd.DataFrame, optional, None): the log dataframe
        """
        mask = self.mask(data_df)
        self.apply(data_df, mask)

        # if a log_df is provided, create a new column for this role and mark the rows that were
        # affected by the rule
        if (log_df is not None) and (len(log_df) == len(mask)):
            log_df[self.identifier] = False
            log_df.loc[mask, self.identifier] = True


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

    def set_id(self, prefix: str):
        """
        sets the identifier of this rulegroup and calls the set_id method of the child rules
        Args:
            prefix: the prefix from the parent in the rule-tree which has to be part of the id
        """
        idx = 1
        self.identifier = f'{prefix}_{self.prefix}'
        for rule in self.rules:
            rule.set_id(f'{self.identifier}_#{idx}')
            idx = idx + 1

    def process(self, data_df: pd.DataFrame, log_df: Optional[pd.DataFrame] = None):
        """
        process the dataframe and apply the rules of this group.
        If a log_df is provided, the rows on which the rule is applied to is added to the log_df.

        Args:
            df (pd.DataFrame) : dataframe on which the rule has to be applied
            log_df (pd.DataFrame, optional, None): the log dataframe
        """
        for rule in self.rules:
            rule.process(data_df=data_df, log_df=log_df)

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

    def collect_description(self, part: str) -> List[DescriptionEntry]:
        """
        Returns the description of this rule elements and its children as a list.

        Args:
            part (str): the part to which the group belongs to

        Returns:
            List (DescriptionEntry): all description elements of the group and its children
        """
        entries: List[DescriptionEntry] = [
            DescriptionEntry(
                part=part,
                type="Group",
                ruleclass="",
                identifier=self.identifier,
                description=self.get_description())]

        for rule in self.rules:
            entries.extend(rule.collect_description(part))

        return entries
