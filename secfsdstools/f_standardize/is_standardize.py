"""Contains the definitions to standardize incaome statements."""
from typing import List

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule
from secfsdstools.f_standardize.base_validation_rules import ValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer


class IncomeStatementStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create IncomeStatements that are comparable,
    meaning that they have the same tags.

    At the end, the standardized IS contains the following columns

    Position	Relation
    Revenues / SalesRevenueNet
      RevenueFromContractWithCustomerExcludingAssessedTax
      OtherSalesRevenueNet


    Cost of Goods and Services Sold

    Gross Profit	                Revenues - Cost of Goods and Services Sold
    Operating Expenses

    Operating Income (Loss)	        Gross Profit - Operating Expenses
    Other Income and Expenses

    Income Before Tax               Operating Income + Other Income and Expenses
    Income Tax Expense (Benefit)

    Net Income (Loss)               Income Before Tax - Income Tax Expense
    """

    is_basic_sumup_rg = RuleGroup(
        prefix="",
        rules=[]
    )

    #     Problem: OtherSalesRevenueNet kann mit SalesREvneues.. auftauchen
    #      aber auch mit RevenueFromContractWithCustomerExcludingAssessedTax
    #      Other dürfte nicht alleine summiert werden
    #
    #     in der sumup rule müsste eine zweite Liste vorhanden sein, can be present :)
    #
    #     OtherSalesRevenue darf nur addiert werden, wenn SalesRevenueGoods/Service vorhanden sind
    # oder mit RevenueFromContractWithCustomerExcludingAssessedTax

    is_revenue_rg = RuleGroup(
        prefix="Rev",
        rules=[
            SumUpRule(sum_tag='SalesRevenueNet',
                      potential_summands=[
                          'SalesRevenueGoodsNet',
                          'SalesRevenueServicesNet'],
                      optional_summands=[
                          'OtherSalesRevenueNet'
                      ]),
            CopyTagRule(original='SalesRevenueNet', target='Revenues'),
            SumUpRule(sum_tag='RevenuesSum',
                      potential_summands=[
                          'RevenueFromContractWithCustomerExcludingAssessedTax'],
                      optional_summands=[
                          'OtherSalesRevenueNet'
                      ]),
            CopyTagRule(original='RevenuesSum', target='Revenues'),
            CopyTagRule(original='InterestAndDividendIncomeOperating', target='Revenues')
        ]
    )

    main_rule_tree = RuleGroup(prefix="IS",
                               rules=[
                                   is_revenue_rg
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
    final_tags: List[str] = ['Revenues',
                             'RevenuesSum',
                             'CostOfGoodsAndServicesSold',
                             'GrossProfit',
                             'OperatingIncomeLoss',
                             'IncomeLossBeforeIncomeTaxExpenseBenefit',
                             'IncomeTaxExpenseBenefit',
                             'NetIncomeLoss'
                             ]

    # used to evaluate if a report is the main balancesheet report
    # inside a report, there can be several tables (different report nr)
    # which stmt value is BS.
    # however, we might be only interested in the "major" BS report. Usually this is the
    # one which has the least nan in the following columns
    main_statement_tags = ['Revenues',
                           'SalesRevenueNet',
                           'SalesRevenueGoodsNet',
                           'SalesRevenueServicesNet',
                           'CostOfRevenue',
                           'CostOfGoodsAndServicesSold',
                           'CostOfGoodsSold',
                           'CostOfServices',
                           'GrossProfit',
                           'OperatingExpenses',
                           'OperatingIncomeLoss',
                           'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
                           'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
                           'IncomeLossBeforeIncomeTaxExpenseBenefit',
                           'IncomeTaxExpenseBenefit',
                           'NetIncomeLoss',
                           'ProfitLoss'
                           ]

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
