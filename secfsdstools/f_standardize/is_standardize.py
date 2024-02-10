"""Contains the definitions to standardize incaome statements."""
from typing import List

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule, \
    missingsumparts_rules_creator, SetSumIfOnlyOneSummand, PostCopyToFirstSummand, \
    PreSumUpCorrection, PostSetToZero
from secfsdstools.f_standardize.base_validation_rules import ValidationRule, SumValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer


class IncomeStatementStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create IncomeStatements that are comparable,
    meaning that they have the same tags.

    At the end, the standardized IS contains the following columns
        .. TBD

    """


    main_rule_tree = RuleGroup(prefix="IS",
                               rules=[
                               ])

    preprocess_rule_tree = RuleGroup(prefix="IS_PRE",
                                     rules=[
                                     ])

    post_rule_tree = RuleGroup(prefix="IS_POST",
                               rules=[
                               ])

    validation_rules: List[ValidationRule] = [
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = [
                             ]

    # used to evaluate if a report is the main balancesheet report
    # inside a report, there can be several tables (different report nr)
    # which stmt value is BS.
    # however, we might be only interested in the "major" BS report. Usually this is the
    # one which has the least nan in the following columns
    main_statement_tags = []

    def __init__(self, filter_for_main_statement: bool = True, iterations: int = 3):
        super().__init__(
            pre_rule_tree=self.preprocess_rule_tree,
            main_rule_tree=self.main_rule_tree,
            post_rule_tree=self.post_rule_tree,
            validation_rules=self.validation_rules,
            final_tags=self.final_tags,
            main_iterations=iterations,
            filter_for_main_statement=filter_for_main_statement,
            main_statement_tags=self.main_statement_tags)
