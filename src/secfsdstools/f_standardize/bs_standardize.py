"""Contains the definitions to standardize balance sheets."""
from typing import List, Optional

from secfsdstools.f_standardize.base_prepivot_rules import PrePivotDeduplicate
from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_rules import (
    CopyTagRule,
    PostCopyToFirstSummand,
    PostSetToZero,
    PreSumUpCorrection,
    SetSumIfOnlyOneSummand,
    SumUpRule,
    missingsumparts_rules_creator,
)
from secfsdstools.f_standardize.base_validation_rules import SumValidationRule, ValidationRule
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
    Equity
        HolderEquity
            PaidInCapital
            TreasuryStockValue
            RetainedEarnings
        TemporaryEquity
        RedeemableEquity
    LiabilitiesAndEquity

    """
    prepivot_rule_tree = RuleGroup(prefix="BS_PREPIV",
                                     rules=[PrePivotDeduplicate()
                                     ])

    preprocess_rule_tree = RuleGroup(prefix="BS_PRE",
                                     rules=[
                                         # sometimes values are tagged the wrong way.
                                         # there are cases when the real Assets Value is
                                         # tagged as AssetsNoncurrent and vice versa. fix that
                                         PreSumUpCorrection(sum_tag='Assets',
                                                            mixed_up_summand='AssetsNoncurrent',
                                                            other_summand='AssetsCurrent'),
                                         PreSumUpCorrection(sum_tag='Assets',
                                                            mixed_up_summand='AssetsCurrent',
                                                            other_summand='AssetsNoncurrent'),
                                     ])
    bs_rename_rg = RuleGroup(
        prefix="BR",
        rules=[
            # sometimes, the total Assets is tagged as AssetsNet
            CopyTagRule(original='AssetsNet', target='Assets'),
            # StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest
            # has precedence over StockholdersEquity
            CopyTagRule(original='CashAndCashEquivalentsAtCarryingValue', target='Cash'),
            CopyTagRule(original='LiabilitiesAndStockholdersEquity',
                        target='LiabilitiesAndEquity'),
            # most of the time, RetainedEarningsAccumulatedDeficit is used
            CopyTagRule(original='RetainedEarningsAccumulatedDeficit', target='RetainedEarnings')
        ]
    )

    bs_owner_equity = RuleGroup(
        prefix='EQ',
        rules=[
            CopyTagRule(
                original='StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                target='HolderEquity'),
            # either there is a StockholderEquity tag or a PartnersCapital tag,
            # but both never appear together
            CopyTagRule(original='PartnersCapital', target='HolderEquity'),
            CopyTagRule(original='StockholdersEquity', target='HolderEquity'),
            # often, there is also a TemporaryEquityCarryingAmountAttributableToParent
            # which is part of Equity
            SumUpRule(
                sum_tag='TemporaryEquity',
                potential_summands=[
                    'TemporaryEquityAggregateAmountOfRedemptionRequirement',
                    'TemporaryEquityCarryingAmountAttributableToParent',
                    'TemporaryEquityRedemptionAmountAttributableToParent',
                    'TemporaryEquityRedemptionAmountAttributableToNoncontrollingInterest',
                ]
            ),
            SumUpRule(
                sum_tag='RedeemableEquity',
                potential_summands=[
                    'RedeemableNoncontrollingInterestEquityCarryingAmount',
                    'RedeemableNoncontrollingInterestEquityRedemptionAmount',
                    'RedeemableNoncontrollingInterestEquityOtherCarryingAmount',
                    'RedeemableNoncontrollingInterestEquityOtherRedemptionAmount',
                    'RedeemablePreferredStockEquityOtherCarryingAmount',
                    'RedeemablePreferredStockEquityOtherRedemptionAmount',
                ]
            ),
            SumUpRule(
                sum_tag='Equity',
                potential_summands=[
                    'HolderEquity',
                    'TemporaryEquity',
                    'RedeemableEquity'
                ]
            )
        ]
    )

    bs_sum_completion_rg = RuleGroup(
        prefix="SC",
        rules=[
            # if only one tag of these are missing, calculate the missing one
            *missingsumparts_rules_creator(
                sum_tag='Assets',
                summand_tags=['AssetsCurrent', 'AssetsNoncurrent']
            ),
            # if only one tag of these are missing, calculate the missing one
            *missingsumparts_rules_creator(
                sum_tag='Liabilities',
                summand_tags=['LiabilitiesCurrent', 'LiabilitiesNoncurrent']
            ),
            # if only one tag of these are missing, calculate the missing one
            *missingsumparts_rules_creator(
                sum_tag='Assets',
                summand_tags=['Liabilities', 'Equity']
            ),
            # if only one tag of these are missing, calculate the missing one
            *missingsumparts_rules_creator(
                sum_tag='LiabilitiesAndEquity',
                summand_tags=['Liabilities', 'Equity']
            )
        ])

    bs_sumup_rg = RuleGroup(
        # tries to create missing major tags by summing up potential sub tags of the tag
        prefix="SU",
        rules=[
            # if there was now CashAndCashEquivalentsAtCarryingValue tag, sum up these tags into the
            # Cash tag
            SumUpRule(
                sum_tag='Cash',
                potential_summands=[
                    'CashAndCashEquivalentsAtFairValue',
                    'CashAndDueFromBanks',
                    'CashCashEquivalentsAndFederalFundsSold',
                    'RestrictedCashAndCashEquivalentsAtCarryingValue',
                    'CashAndCashEquivalentsInForeignCurrencyAtCarryingValue']),
            # if there is not RetainedEarnings  tag or RetainedEarningsAccumulatedDeficit
            # sum up these to RetainedEarnings
            SumUpRule(
                sum_tag='RetainedEarnings',
                potential_summands=[
                    'RetainedEarningsUnappropriated',
                    'RetainedEarningsAppropriated']),
            SumUpRule(
                sum_tag='LongTermDebt',
                potential_summands=[
                    'LongTermDebtNoncurrent',
                    'LongTermDebtAndCapitalLeaseObligations',
                ]
            ),
            SumUpRule(
                sum_tag='LiabilitiesNoncurrent',
                potential_summands=[
                    'AccruedIncomeTaxesNoncurrent',
                    'DeferredAndPayableIncomeTaxes',
                    'DeferredIncomeTaxesAndOtherLiabilitiesNoncurrent',
                    'DeferredIncomeTaxLiabilitiesNet',
                    'DeferredTaxLiabilitiesNoncurrent',
                    'DefinedBenefitPensionPlanLiabilitiesNoncurrent',
                    'DerivativeLiabilitiesNoncurrent',
                    'FinanceLeaseLiabilityNoncurrent',
                    'LiabilitiesOtherThanLongtermDebtNoncurrent',
                    'LiabilitiesSubjectToCompromise',
                    'LiabilityForUncertainTaxPositionsNoncurrent',
                    'LongTermDebt',
                    'LongTermRetirementBenefitsAndOtherLiabilities',
                    'OperatingLeaseLiabilityNoncurrent',
                    'OtherLiabilitiesNoncurrent',
                    'OtherPostretirementDefinedBenefitPlanLiabilitiesNoncurrent',
                    'PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent',
                    'RegulatoryLiabilityNoncurrent',
                    'SelfInsuranceReserveNoncurrent',
                ]
            ),
        ]
    )

    bs_setsum_rg = RuleGroup(
        # set the Sum Tag if only one of the summands is present
        prefix="SetSum",
        rules=[
            # if there is only AssetsCurrent, set Assets to the same value and set
            # AssetsNoncurrent to 0
            SetSumIfOnlyOneSummand(
                sum_tag='Assets',
                summand_set='AssetsCurrent',
                summands_nan=['AssetsNoncurrent']
            ),
            # if there is only AssetsNoncurrent, set Assets to the same value and set
            # AssetsCurrent to 0
            SetSumIfOnlyOneSummand(
                sum_tag='Assets',
                summand_set='AssetsNoncurrent',
                summands_nan=['AssetsCurrent']
            ),
            # if there is only LiabilitiesCurrent, set Liabilities to the same value and set
            # LiabilitiesNoncurrent to 0
            SetSumIfOnlyOneSummand(
                sum_tag='Liabilities',
                summand_set='LiabilitiesCurrent',
                summands_nan=['LiabilitiesNoncurrent']
            ),
            # if there is only LiabilitiesNoncurrent, set Liabilities to the same value and set
            # LiabilitiesCurrent to 0
            SetSumIfOnlyOneSummand(
                sum_tag='Liabilities',
                summand_set='LiabilitiesNoncurrent',
                summands_nan=['LiabilitiesCurrent']
            ),
        ]
    )

    main_rule_tree = RuleGroup(prefix="BS",
                               rules=[
                                   bs_rename_rg,
                                   bs_owner_equity,
                                   bs_sum_completion_rg,
                                   bs_sumup_rg,
                                   bs_setsum_rg
                               ])

    post_rule_tree = RuleGroup(prefix="BS_POST",
                               rules=[
                                   # if only Assets is sets, set the AssetsCurrent to value
                                   # of Assets and AssetsNoncurrent to 0
                                   PostCopyToFirstSummand(sum_tag='Assets',
                                                          first_summand='AssetsCurrent',
                                                          other_summands=[
                                                              'AssetsNoncurrent']),
                                   # if only Liabilities is sets, set the LiabilitiesCurrent to
                                   # value of Liabilities and LiabilitiesNoncurrent to 0
                                   PostCopyToFirstSummand(sum_tag='Liabilities',
                                                          first_summand='LiabilitiesCurrent',
                                                          other_summands=[
                                                              'LiabilitiesNoncurrent']),
                                   # if none of these tags is present, set them to 0
                                   PostSetToZero(
                                       tags=['Assets', 'AssetsCurrent', 'AssetsNoncurrent']),
                                   # if none of these tags is present, set them to 0
                                   PostSetToZero(
                                       tags=['Liabilities', 'LiabilitiesCurrent',
                                             'LiabilitiesNoncurrent']),
                                   PostSetToZero(tags=['TemporaryEquity']),
                                   PostSetToZero(tags=['RedeemableEquity']),
                                   PostSetToZero(tags=['AdditionalPaidInCapital']),
                                   PostSetToZero(tags=['TreasuryStockValue']),
                               ])

    validation_rules: List[ValidationRule] = [
        SumValidationRule(identifier='AssetsCheck',
                          sum_tag='Assets',
                          summands=['AssetsCurrent', 'AssetsNoncurrent']),
        SumValidationRule(identifier='LiabilitiesCheck',
                          sum_tag='Liabilities',
                          summands=['LiabilitiesCurrent', 'LiabilitiesNoncurrent']),
        SumValidationRule(identifier='EquityCheck',
                          sum_tag='LiabilitiesAndEquity',
                          summands=['Equity', 'Liabilities']),
        SumValidationRule(identifier='AssetsLiaEquCheck',
                          sum_tag='Assets',
                          summands=['Equity', 'Liabilities']),
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = ['Assets', 'AssetsCurrent', 'Cash', 'AssetsNoncurrent',
                             'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
                             'Equity',
                             'HolderEquity',
                             'RetainedEarnings',
                             'AdditionalPaidInCapital',
                             'TreasuryStockValue',
                             'TemporaryEquity',
                             'RedeemableEquity',
                             'LiabilitiesAndEquity',
                             ]

    # used to evaluate if a report is the main balancesheet report
    # inside a report, there can be several tables (different report nr)
    # which stmt value is BS.
    # however, we might be only interested in the "major" BS report. Usually this is the
    # one which has the least nan in the following columns
    main_statement_tags = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                           'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent']

    def __init__(self,
                 prepivot_rule_tree: Optional[RuleGroup] = None,
                 pre_rule_tree: Optional[RuleGroup] = None,
                 main_rule_tree: Optional[RuleGroup] = None,
                 post_rule_tree: Optional[RuleGroup] = None,
                 validation_rules: Optional[List[ValidationRule]] = None,
                 final_tags: Optional[List[str]] = None,
                 main_statement_tags: Optional[List[str]] = None,

                 filter_for_main_statement: bool = True,
                 main_iterations: int = 3,
                 invert_negated: bool = True,
                 additional_final_sub_fields: Optional[List[str]] = None,
                 additional_final_tags: Optional[List[str]] = None):
        """
        Initialize the Income Statement Standardizer.

        Fine tune it with the following arguments:

        Args:
            prepivot_rule_tree: rules that are applied before the data is pivoted. These are rules
                    that pathfilter (like deduplicate) or correct values.
            pre_rule_tree: rules that are applied once before the main processing. These are mainly
                    rules that try to correct existing data from obvious errors (like wrong
                    tagging)
            main_rule_tree: rules that are applied during the main processing rule and which do the
                    heavy lifting. These rules can be executed multiple times depending on the value
                    of the main_iterations parameter
            post_rule_tree: rules that are used to "cleanup", like setting certain values to
                    0.0. They are just executed once.
            validation_rules: Validation rules are applied after all rules were applied.
                   they add validation columns to the main dataset. Validation rules do check
                   if certain requirements are met. E.g. in a Balance Sheet, the following
                   equation should be true: Assets = AssetsCurrent + AssetsNoncurrent
            final_tags: The list of tags/columns that will appear in the final result dataframe.
            main_statement_tags: list of tags that is used to identify the main table of a
                   financial statement (income statement, balance sheet, cash flow).

            filter_for_main_statement (bool):
                    Only consider the reports that contain most of the "main_statement_tags".
                    Default is True.
            main_iterations (int): Number of times the main rules should be applied.
                    Default is 3 for CashFlow.
            invert_negated (bool, Optional, True): inverts the value of the tags that are marked
                   as negated (in the pre_df).
            additional_final_sub_fields (List, Optional):
                    When using the present method, the results are joined with the following fields
                    from the sub_df entry: 'adsh', 'cik', 'form', 'fye', 'fy', 'fp', 'filed'
                    Additional fields can be assigend in this list. Default is None.
            additional_final_tags (List, Optional):
                     the "final_tags" list define the tags that will be present in the final result
                     dataframe. Additional tags can be added via this parameter. Default is None.
        """
        super().__init__(
            prepivot_rule_tree=
                    prepivot_rule_tree if prepivot_rule_tree else self.prepivot_rule_tree,
            pre_rule_tree=pre_rule_tree if pre_rule_tree else self.preprocess_rule_tree,
            main_rule_tree=main_rule_tree if main_rule_tree else self.main_rule_tree,
            post_rule_tree=post_rule_tree if post_rule_tree else self.post_rule_tree,
            validation_rules=validation_rules if validation_rules else self.validation_rules,
            final_tags=final_tags if final_tags else self.final_tags,
            main_statement_tags=
                    main_statement_tags if main_statement_tags else self.main_statement_tags,

            filter_for_main_statement=filter_for_main_statement,
            main_iterations=main_iterations,
            invert_negated=invert_negated,
            additional_final_sub_fields=additional_final_sub_fields,
            additional_final_tags=additional_final_tags
        )
