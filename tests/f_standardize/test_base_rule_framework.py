from typing import List, Set

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_rule_framework import Rule, RuleGroup


class Rule1(Rule):

    def __init__(self, tag_name: str):
        self.tag_name = tag_name

    def get_input_tags(self) -> Set[str]:
        return {self.tag_name}

    def get_target_tags(self) -> List[str]:
        return [self.tag_name]

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        return df[df.columns[0]] == df[df.columns[0]]

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        # do nothing
        pass

    def get_description(self) -> str:
        return ""


class Rule2(Rule):

    def __init__(self, tag1: str, tag2: str):
        self.tag1 = tag1
        self.tag2 = tag2

    def get_input_tags(self) -> Set[str]:
        return {self.tag1, self.tag2}

    def get_target_tags(self) -> List[str]:
        return [self.tag1, self.tag2]

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        return df[df.columns[0]] == df[df.columns[0]]

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        # do nothing
        pass

    def get_description(self) -> str:
        return ""


def test_simple_rule():
    rule = Rule1(tag_name='Tag1')
    rule.set_id(prefix="R")

    assert rule.get_input_tags() == {'Tag1'}
    assert rule.get_target_tags() == ['Tag1']
    assert rule.get_target_tags_str() == 'Tag1'
    assert rule.id == "R_Tag1"

    data = [10, 20, 30, 40, 50, 60]
    df = pd.DataFrame(data, columns=['Numbers'])

    # call process without a log -> simply expect no problems
    rule.process(df=df)

    log_df = df.copy()
    # call rule with log.df
    rule.process(df=df, log_df=log_df)

    # we expect a colum with the rule.id as columname
    # the sum of that column has to be the length of the dataframe
    assert log_df[rule.id].sum() == len(df)


def test_multitag_rule():
    rule = Rule2(tag1='Tag1', tag2='Tag2')
    rule.set_id(prefix="R")

    assert rule.get_input_tags() == {'Tag1', 'Tag2'}
    assert set(rule.get_target_tags()) == {'Tag1', 'Tag2'}
    assert rule.get_target_tags_str() == 'Tag1/Tag2'
    assert rule.id == "R_Tag1/Tag2"


def test_simple_group_rule():
    rule1 = Rule1(tag_name='Tag1')
    rule2 = Rule1(tag_name='Tag2')

    rulegroup = RuleGroup(prefix="RG", rules=[rule1, rule2])
    rulegroup.set_id(parent_prefix="R")

    assert rule1.id == "R_RG_#1_Tag1"
    assert rule2.id == "R_RG_#2_Tag2"
    assert rulegroup.get_input_tags() == {'Tag1', 'Tag2'}

    data = [10, 20, 30, 40, 50, 60]
    df = pd.DataFrame(data, columns=['Numbers'])

    # call process without a log -> simply expect no problems
    rulegroup.process(df=df)

    log_df = df.copy()
    # call rule with log.df
    rulegroup.process(df=df, log_df=log_df)

    # we expect colums named with the id of both rules.
    # the sum of these column has to be the length of the dataframe
    assert log_df[rule1.id].sum() == len(df)
    assert log_df[rule2.id].sum() == len(df)