"""Contains the definitions to standardize incaome statements."""
from typing import List

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_validation_rules import ValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer


class CashFlowStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create CashFlow statements that are comparable,
    meaning that they have the same tags.

    At the end, the standardized CF contains the following columns

    <pre>

    </pre>

    Detailled View:
    <pre>
      Main Rules

      Post Rule (Cleanup)

    </pre>
    """
    prepivot_rule_tree = RuleGroup(
        prefix="CF_PREPIV",
        rules=[]
    )

    main_rule_tree = RuleGroup(prefix="CF",
                               rules=[
                               ])

    preprocess_rule_tree = RuleGroup(prefix="CF_PRE",
                                     rules=[
                                     ])

    post_rule_tree = RuleGroup(
        prefix="CF",
        rules=[
        ])

    validation_rules: List[ValidationRule] = [
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = [
    ]

    # used to evaluate if a report is the main cashflow report
    # inside a report, there can be several tables (different report nr)
    # which stmt value is CF.
    # however, we might be only interested in the "major" CF report. Usually this is the
    # one which has the least nan in the following columns
    main_statement_tags = [
    ]

    def __init__(self, filter_for_main_statement: bool = True, iterations: int = 3):
        super().__init__(
            prepivot_rule_tree=self.prepivot_rule_tree,
            pre_rule_tree=self.preprocess_rule_tree,
            main_rule_tree=self.main_rule_tree,
            post_rule_tree=self.post_rule_tree,
            validation_rules=self.validation_rules,
            final_tags=self.final_tags,
            main_iterations=iterations,
            filter_for_main_statement=filter_for_main_statement,
            main_statement_tags=self.main_statement_tags)
