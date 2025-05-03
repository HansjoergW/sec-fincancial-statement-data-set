"""
This module defines a simple rule framework that allows to define concrete rules and build
a rule hierarchy that then is used in the standardizer.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Set

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

    def process(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the rules on the provided dataframe.

        Args:
            data_df (pd.DataFrame): the dataframe on which the rules will be applied
        Returns:
            pd.DataFrame: make the process chainable
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
    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> pd.DataFrame:
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so now new dataframe is produced.

        Args:
            data_df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """

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
    """Base class to define a single rule that is applied before the dataframe was pivoted"""

    index_cols = ['adsh', 'coreg', 'report', 'ddate', 'qtrs', 'tag', 'version']

    def __init__(self, rule_id: str):
        self.rule_id = rule_id

        # pre pivot rules keep the applied log within the class to concat it later
        self.log_df: pd.DataFrame = pd.DataFrame(columns=self.index_cols + ['id'])

    def set_id(self, prefix: str):
        """
        sets the identifier of this rule entity
        Args:
            prefix: the prefix from the parent in the rule-tree which has to be part of the id
        """
        self.identifier = f'{prefix}_{self.rule_id}'

    def process(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        process the dataframe and apply the rule.

        Args:
            data_df (pd.DataFrame) : dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """
        mask = self.mask(data_df)

        # since apply can also change the data_df, we have to create the log changes, before
        # the rule is applied
        self.log_df = data_df[mask][self.index_cols].copy()
        self.log_df['id'] = self.identifier

        return self.apply(data_df, mask)

    def get_input_tags(self) -> Set[str]:
        return set()  # default value for PrePivotRules


class Rule(AbstractRule):
    """Base class to define a single rule that is used after the dataframe was pivoted"""
    masked: pd.Series

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

    def process(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        process the dataframe and apply the rule.

        Args:
            data_df (pd.DataFrame) : dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """
        mask = self.mask(data_df)
        result = self.apply(data_df, mask)
        self.masked = mask
        return result

    def get_mask(self) -> pd.Series:
        """
        returns the a pandas.Series object that marks the masked
        entries.
        """
        return self.masked


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

    def process(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        process the dataframe and apply the rules of this group.

        Args:
            df (pd.DataFrame) : dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """
        current_df = data_df
        for rule in self.rules:
            current_df = rule.process(data_df=current_df)
        return current_df

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

    def collect_masked_entries(self, ids: List[str], masks: List[pd.Series]):
        """
        internal helper method which helps to recursively traverse down the rule group structure
        and to collect the applied rules together with their ids
        Args:
            ids: list with ids
            masks: list with the applied masks

        """
        for rule in self.rules:
            if isinstance(rule, RuleGroup):
                rule.collect_masked_entries(ids, masks)
            elif isinstance(rule, Rule):
                ids.append(rule.identifier)
                masks.append(rule.get_mask())

    def append_log(self, log_df: pd.DataFrame) -> pd.DataFrame:
        """
        append the rules that were applied within this group to the provided log_df and returns
        the extended version of the log dataframe.

        Args:
            log_df: log dataframe to which the information should be added

        Returns:
            pd.DataFrame: the extended dataframe

        """
        ids: List[str] = []
        masks: List[pd.Series] = []

        self.collect_masked_entries(ids, masks)

        return pd.concat([log_df] + [s.rename(col_name) for col_name, s in zip(ids, masks)], axis=1)
