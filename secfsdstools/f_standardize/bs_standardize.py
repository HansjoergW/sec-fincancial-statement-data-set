"""Contains the definitions to standardize balance sheets."""
from typing import List

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule, \
    missingsumparts_rules_creator, SetSumIfOnlyOneSummand, PostCopyToFirstSummand, \
    PreSumUpCorrection
from secfsdstools.f_standardize.base_validation_rules import ValidationRule, SumValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer


class BalanceSheetStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create BalanceSheets that are compareable,
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
        RetainedEarnings
    LiabilitiesAndOwnerEquity

    """

    bs_rename_rg = RuleGroup(
        rules=[
            # sometimes, the total Assets is contained in the AssetsNet tag
            CopyTagRule(original='AssetsNet', target='Assets'),
            # either there is a StockholderEquity tag or a PartnersCapital tag,
            # but both to never appear together
            CopyTagRule(original='PartnersCapital', target='OwnerEquity'),
            CopyTagRule(original='StockholdersEquity', target='OwnerEquity'),
            CopyTagRule(original='CashAndCashEquivalentsAtCarryingValue', target='Cash'),
            CopyTagRule(original='LiabilitiesAndStockholdersEquity',
                        target='LiabilitiesAndOwnerEquity')

            # RenameRule(
            #     original='StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
            #     target='Equity'),
            # RenameRule(original='RetainedEarningsAppropriated', target='RetainedEarnings'),
            # RenameRule(original='RetainedEarningsAccumulatedDeficit', target='RetainedEarnings')
        ],
        prefix="BR"
    )

    # todo: CashOther wird noch nirgends verwendet
    bs_sumup_rg = RuleGroup(
        rules=[SumUpRule(
            sum_tag='CashOther',
            potential_summands=[
                'CashAndCashEquivalentsAtFairValue',
                'CashAndDueFromBanks',
                'CashCashEquivalentsAndFederalFundsSold',
                'RestrictedCashAndCashEquivalentsAtCarryingValue',
                'CashAndCashEquivalentsInForeignCurrencyAtCarryingValue'])],
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
                                                              'LiabilitiesNoncurrent'])])

    validation_rules: List[ValidationRule] = [
        SumValidationRule(identifier='AssetsCheck',
                          sum_tag='Assets',
                          summands=['AssetsCurrent', 'AssetsNoncurrent']),
        SumValidationRule(identifier='LiabilitiesCheck',
                          sum_tag='Liabilities',
                          summands=['LiabilitiesCurrent', 'LiabilitiesNoncurrent'])
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                             'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
                             'OwnerEquity', 'LiabilitiesAndOwnerEquity',
                             'InventoryNet',
                             'Cash',
                             'CashOther',
                             'RetainedEarnings'
                             ]

    # used to evaluate if a report is the main balancesheet report
    # inside a report, there can be several different tables (different report nr)
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

#
# class BalanceSheetStandardizerOld:
#
#     def fill_still_isna_summands(self, df: pd.DataFrame):
#         """ if we are left with unset summands"""
#
#         # for:
#         # 'Assets': ('AssetsCurrent', 'AssetsNoncurrent'),
#         # 'Liabilities': ('LiabilitiesCurrent', 'LiabilitiesNoncurrent')
#         for total, summands in BalanceSheetStandardizerOld.sum_definition.items():
#             summand_1 = summands[0]
#             summand_2 = summands[1]
#
#             # if only the total is present, we assume that only the first summand
#             # (eg. currentassets are present)
#             # that is not really proper.
#             mask_total_nosum1_nosum2 = (~df[total].isna() &
#                                         df[summand_1].isna() &
#                                         df[summand_2].isna())
#             df.loc[mask_total_nosum1_nosum2, summand_1] = df[total]
#             df.loc[mask_total_nosum1_nosum2, summand_2] = 0.0
#
#             # if only the first summand is present, wie assume that the total is equal
#             # to the first summand and that the second summand is 0
#             mask_nototal_sum1_nosum2 = (df[total].isna() &
#                                         ~df[summand_1].isna() &
#                                         df[summand_2].isna())
#             df.loc[mask_nototal_sum1_nosum2, total] = df[summand_1]
#             df.loc[mask_nototal_sum1_nosum2, summand_2] = 0.0
#
#             # if only the second summand is present, wie assume that the total is equal
#             # to the second summand and that the first  summand is 0
#             mask_nototal_nosum1_sum2 = (df[total].isna() &
#                                         df[summand_1].isna() &
#                                         ~df[summand_2].isna())
#             df.loc[mask_nototal_nosum1_sum2, total] = df[summand_2]
#             df.loc[mask_nototal_nosum1_sum2, summand_1] = 0.0
#
#     def complete_special_rules(self, df: pd.DataFrame):
#         """
#         Special rules which help to complete the information
#         """
#
#         # self._basic_sum_completion(df, total='Assets', summand_1='Liabilities',
#         #                            summand_2='Equity')
#         #
#         # # actually, LiabilitiesAndStockholdersEquity should have the same value as Assets
#         # self._basic_sum_completion(df, total='LiabilitiesAndStockholdersEquity',
#         #                            summand_1='Liabilities',
#         #                            summand_2='Equity')
#
#         # run complete sum again, since total fields could be present now
#         # for:
#         # 'Assets': ('AssetsCurrent', 'AssetsNoncurrent'),
#         # 'Liabilities': ('LiabilitiesCurrent', 'LiabilitiesNoncurrent')
#         self.complete_sums(df)
#
#         self.fill_still_isna_summands(df)
#
#         # if Equity == -Liabilities and Assets is nan, then
#         # Assets, AssetsCurrent, and AssetsNoncurrent is 0.0
#         mask = (df.Equity == -df.Liabilities) & df.Assets.isna()
#         df.loc[mask, 'Assets'] = 0.0
#         df.loc[mask, 'AssetsCurrent'] = 0.0
#         df.loc[mask, 'AssetsNoncurrent'] = 0.0
#
#         # if Cash is not set, then use CashOther to set Cash
#         mask = df.Cash.isna()
#         df.loc[mask, 'Cash'] = df.CashOther
#
#         # set to zero
#         mask = df.InventoryNet.isna()
#         df.loc[mask, 'InventoryNet'] = 0.0
