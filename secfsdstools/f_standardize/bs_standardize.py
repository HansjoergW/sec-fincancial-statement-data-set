"""Contains the definitions to standardize balance sheets."""
from typing import List

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule, \
    missingsumparts_rules_creator, SetSumIfOnlyOneSummand, PostCopyToFirstSummand, \
    PreSumUpCorrection, PostSetToZero
from secfsdstools.f_standardize.base_validation_rules import ValidationRule, SumValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer


class BalanceSheetStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create BalanceSheets that are comparable,
    meaning that they have the same tags.

    At the end, the standardized BS contains the following columns
    Assets
       AssetsCurrent
           Cash
       AssetsNoncurrent
    Liabilities
       LiabilitiesCurrent
       LiabilitiesNoncurrent
    OwnerEquity
        PaidInCapital
        TreasuryStockValue
        RetainedEarnings
    LiabilitiesAndOwnerEquity

    """

    bs_rename_rg = RuleGroup(
        rules=[
            # sometimes, the total Assets is contained in the AssetsNet tag
            CopyTagRule(original='AssetsNet', target='Assets'),
            # StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest
            # has precedence over StockholdersEquity
            CopyTagRule(
                original='StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                target='OwnerEquity'),
            # either there is a StockholderEquity tag or a PartnersCapital tag,
            # but both to never appear together
            CopyTagRule(original='PartnersCapital', target='OwnerEquity'),
            CopyTagRule(original='StockholdersEquity', target='OwnerEquity'),
            CopyTagRule(original='CashAndCashEquivalentsAtCarryingValue', target='Cash'),
            CopyTagRule(original='LiabilitiesAndStockholdersEquity',
                        target='LiabilitiesAndOwnerEquity'),
            # most of the time, RetainedEarningsAccumulatedDeficit is used
            CopyTagRule(original='RetainedEarningsAccumulatedDeficit', target='RetainedEarnings')
        ],
        prefix="BR"
    )

    # todo: CashOther wird noch nirgends verwendet
    bs_sumup_rg = RuleGroup(
        rules=[
            SumUpRule(
                sum_tag='Cash',
                potential_summands=[
                    'CashAndCashEquivalentsAtFairValue',
                    'CashAndDueFromBanks',
                    'CashCashEquivalentsAndFederalFundsSold',
                    'RestrictedCashAndCashEquivalentsAtCarryingValue',
                    'CashAndCashEquivalentsInForeignCurrencyAtCarryingValue']),
            SumUpRule(
                sum_tag='RetainedEarnings',
                potential_summands=[
                    'RetainedEarningsUnappropriated',
                    'RetainedEarningsAppropriated'])
        ],
        prefix="SU"
    )

    bs_sum_completion = RuleGroup(
        prefix="SC",
        rules=[
            *missingsumparts_rules_creator(
                sum_tag='Assets',
                summand_tags=['AssetsCurrent', 'AssetsNoncurrent']
            ),
            SetSumIfOnlyOneSummand(
                sum_tag='Assets',
                summand_set='AssetsCurrent',
                summands_nan=['AssetsNoncurrent']
            ),
            SetSumIfOnlyOneSummand(
                sum_tag='Assets',
                summand_set='AssetsNoncurrent',
                summands_nan=['AssetsCurrent']
            ),
            *missingsumparts_rules_creator(
                sum_tag='Liabilities',
                summand_tags=['LiabilitiesCurrent', 'LiabilitiesNoncurrent']
            ),
            SetSumIfOnlyOneSummand(
                sum_tag='Liabilities',
                summand_set='LiabilitiesCurrent',
                summands_nan=['LiabilitiesNoncurrent']
            ),
            SetSumIfOnlyOneSummand(
                sum_tag='Liabilities',
                summand_set='LiabilitiesNoncurrent',
                summands_nan=['LiabilitiesCurrent']
            ),
            *missingsumparts_rules_creator(
                sum_tag='AssLiaOwe',
                summand_tags=['Liabilities', 'OwnerEquity']
            ),
            *missingsumparts_rules_creator(
                sum_tag='LiabilitiesAndOwnerEquity',
                summand_tags=['Liabilities', 'OwnerEquity']
            )
        ])

    main_rule_tree = RuleGroup(prefix="BS",
                               rules=[
                                   bs_rename_rg,
                                   bs_sumup_rg,
                                   bs_sum_completion,
                               ])

    preprocess_rule_tree = RuleGroup(prefix="BS_PRE",
                                     rules=[
                                         PreSumUpCorrection(sum_tag='Assets',
                                                            mixed_up_summand='AssetsNoncurrent',
                                                            other_summand='AssetsCurrent'),
                                         PreSumUpCorrection(sum_tag='Assets',
                                                            mixed_up_summand='AssetsCurrent',
                                                            other_summand='AssetsNoncurrent'),
                                     ])

    post_rule_tree = RuleGroup(prefix="BS_POST",
                               rules=[
                                   PostCopyToFirstSummand(sum_tag='Assets',
                                                          first_summand='AssetsCurrent',
                                                          other_summands=[
                                                              'AssetsNoncurrent']),
                                   PostCopyToFirstSummand(sum_tag='Liabilities',
                                                          first_summand='LiabilitiesCurrent',
                                                          other_summands=[
                                                              'LiabilitiesNoncurrent']),
                                   PostSetToZero(
                                       tags=['Assets', 'AssetsCurrent', 'AssetsNoncurrent']),
                                   PostSetToZero(
                                       tags=['Liabilities', 'LiabilitiesCurrent',
                                             'LiabilitiesNoncurrent'])
                               ])

    validation_rules: List[ValidationRule] = [
        SumValidationRule(identifier='AssetsCheck',
                          sum_tag='Assets',
                          summands=['AssetsCurrent', 'AssetsNoncurrent']),
        SumValidationRule(identifier='LiabilitiesCheck',
                          sum_tag='Liabilities',
                          summands=['LiabilitiesCurrent', 'LiabilitiesNoncurrent']),
        SumValidationRule(identifier='OwnerEquityCheck',
                          sum_tag='LiabilitiesAndOwnerEquity',
                          summands=['OwnerEquity', 'Liabilities']),
        SumValidationRule(identifier='AssetsLiaEquCheck',
                          sum_tag='Assets',
                          summands=['OwnerEquity', 'Liabilities']),
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                             'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
                             'OwnerEquity', 'LiabilitiesAndOwnerEquity',
                             'Cash',
                             'RetainedEarnings',
                             'AdditionalPaidInCapital',
                             'TreasuryStockValue'
                             ]

    # used to evaluate if a report is the main balancesheet report
    # inside a report, there can be several tables (different report nr)
    # which stmt value is BS.
    # however, we might be only interested in the "major" BS report. Usually this is the
    # one which has the least nan in the following columns
    main_statement_tags = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                           'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent']

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
