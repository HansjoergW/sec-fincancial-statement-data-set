"""Contains the definitions to standardize incaome statements."""
from typing import List, Set

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_prepivot_rules import PrePivotDeduplicate
from secfsdstools.f_standardize.base_rule_framework import RuleGroup, Rule
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule, PostSetToZero, \
    MissingSumRule
from secfsdstools.f_standardize.base_validation_rules import ValidationRule, SumValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer


class PreCorrectMixUpContinuingOperations(Rule):
    """
    Normally,
    NetCashProvidedByUsedInFinancingActivities,
    NetCashProvidedByUsedInOperatingActivities, and
    NetCashProvidedByUsedInInvestingActivities

    Sum up in NetCashProvidedByUsedInContinuingOperations.

    However, there are reports which use NetCashProvidedByUsedInContinuingOperations to reflect
    NetCashProvidedByUsedInOperatingActivities.

    This rules correct such cases by looking for reports where
    NetCashProvidedByUsedInContinuingOperations and NetCashProvidedByUsedInFinancingActivities are
    set, but not NetCashProvidedByUsedInOperatingActivities.
    """

    def __init__(self):
        """

        Args:
            sum_tag: the tag that should contain the sum
            mixed_up_summand: the summand that actually does contain the sum
            other_summand: the other summand
        """
        self.opcont = 'NetCashProvidedByUsedInContinuingOperations'
        self.opact = 'NetCashProvidedByUsedInOperatingActivities'
        self.finact = 'NetCashProvidedByUsedInFinancingActivities'

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns
        """
        return [self.opact]

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        return {self.opcont, self.opact, self.finact}

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            data_df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        return ~data_df[self.opcont].isnull() & ~data_df[self.finact].isnull() \
                & data_df[self.opact].isnull()

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]):
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        """

        data_df.loc[mask, self.opact] = data_df[self.opcont]
        data_df.loc[mask, self.opcont] = None

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return (f"Checks for reports where '{self.opcont}' was used instead of {self.opact}."
               f"Looks where {self.opcont} and {self.finact} were set, but ${self.opact} is nan."
               f"In this cases, the value from {self.opcont} is copied to {self.opact} "
               f"and {self.opcont} is set to nan afterwards.")


class CashFlowStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create CashFlow statements that are comparable,
    meaning that they have the same tags.

    At the end, the standardized CF contains the following columns

    <pre>

    Final Tags
        NetCashProvidedByUsedInOperatingActivities
        1.1. AdjustmentsToReconcileNetIncomeLossToCashProvidedByUsedInOperatingActivities
        1.1.1. DepreciationDepletionAndAmortization
        1.1.2. DeferredIncomeTaxExpenseBenefit
        1.1.3. ShareBasedCompensation
        1.1.7. IncreaseDecreaseInAccountsPayable
        1.1.8. IncreaseDecreaseInAccruedLiabilities
        1.2. OtherAdjustmentsToCashProvidedByUsedInOperatingActivities
        1.2.2. InterestPaidNet
        1.2.3. IncomeTaxesPaidNet

        NetCashProvidedByUsedInInvestingActivities
        2.1. Investments in Property, Plant, and Equipment
        2.1.1. PaymentsToAcquirePropertyPlantAndEquipment
        2.1.2. ProceedsFromSaleOfPropertyPlantAndEquipment
        2.2. Investments in Securities
        2.2.1. PaymentsToAcquireInvestments
        2.2.2. ProceedsFromSaleOfInvestments
        2.3. Business Acquisitions and Divestitures
        2.3.1. PaymentsToAcquireBusinessesNetOfCashAcquired
        2.3.2. ProceedsFromDivestitureOfBusinessesNetOfCashDivested
        2.4. OtherCashProvidedByUsedInInvestingActivities
        2.4.1. Investments in Intangible Assets
        2.4.1.1. PaymentsToAcquireIntangibleAssets
        2.4.1.2. ProceedsFromSaleOfIntangibleAssets

        NetCashProvidedByUsedInFinancingActivities
        3.1. Equity Transactions
        3.1.1. ProceedsFromIssuanceOfCommonStock
        3.1.4. PaymentsForRepurchaseOfCommonStock
        3.2. Debt Transactions
        3.2.1. ProceedsFromIssuanceOfDebt
        3.2.2. RepaymentsOfDebt
        3.3. Dividends
        3.3.1. DividendsPaid
        3.4. Leases
        3.4.1. PaymentsOfFinanceLeaseObligations
        3.5. Grants and Other Financing Sources
        3.5.1. ProceedsFromGovernmentGrants

        NetIncreaseDecreaseInCashAndCashEquivalents
        4.1. CashAndCashEquivalentsPeriodIncreaseDecrease
        4.2. EffectOfExchangeRateOnCashAndCashEquivalents


    Details
    1.    NetCashProvidedByUsedInOperatingActivities
    1.1.        AdjustmentsToReconcileNetIncomeLossToCashProvidedByUsedInOperatingActivities
    1.1.1.            DepreciationDepletionAndAmortization
    1.1.2.            DeferredIncomeTaxExpenseBenefit
    1.1.3.            ShareBasedCompensation
    1.1.4.            IncreaseDecreaseInAccountsReceivable
    1.1.5.            IncreaseDecreaseInInventories
    1.1.6.            IncreaseDecreaseInPrepaidDeferredExpenseAndOtherAssets
    1.1.7.            IncreaseDecreaseInAccountsPayable
    1.1.8.            IncreaseDecreaseInAccruedLiabilities
    1.1.9.            IncreaseDecreaseInOtherOperatingActivities
    1.1.10.           OtherNoncashIncomeExpense
    1.2.        OtherAdjustmentsToCashProvidedByUsedInOperatingActivities
    1.2.1.            NetIncomeLoss
    1.2.2.            InterestPaidNet
    1.2.3.            IncomeTaxesPaidNet
    1.2.4.            DividendsReceived
    1.2.5.            InterestReceived
    1.2.6.            IncreaseDecreaseInOperatingCapital
    1.2.7.            PaymentsForAssetRetirementObligations
    1.2.8.            PaymentsForProvisions
    1.2.9.            PaymentsForRestructuring

    2.   NetCashProvidedByUsedInInvestingActivities
    2.1.       Investments in Property, Plant, and Equipment
    2.1.1.           PaymentsToAcquirePropertyPlantAndEquipment
    2.1.2.           ProceedsFromSaleOfPropertyPlantAndEquipment
    2.2.      Investments in Securities
    2.2.1.           PaymentsToAcquireInvestments
    2.2.2.           ProceedsFromSaleOfInvestments
    2.2.3.           PaymentsToAcquireHeldToMaturitySecurities
    2.2.4.           ProceedsFromMaturitiesPrepaymentsAndCallsOfHeldToMaturitySecurities
    2.2.5.           PaymentsToAcquireAvailableForSaleSecurities
    2.2.6.           ProceedsFromSaleOfAvailableForSaleSecurities
    2.2.7.           PaymentsToAcquireTradingSecurities
    2.2.8.           ProceedsFromSaleOfTradingSecurities
    2.3.       Business Acquisitions and Divestitures
    2.3.1.           PaymentsToAcquireBusinessesNetOfCashAcquired
    2.3.2.           ProceedsFromDivestitureOfBusinessesNetOfCashDivested
    2.3.3.           PaymentsMadeInConnectionWithBusinessAcquisitions
    2.3.4.           ProceedsFromBusinessAcquisitionsNetOfCashAcquired
    2.4.       OtherCashProvidedByUsedInInvestingActivities
    2.4.1.           Investments in Intangible Assets
    2.4.1.1.              PaymentsToAcquireIntangibleAssets
    2.4.1.2.              ProceedsFromSaleOfIntangibleAssets
    2.4.2.           Loans and Advances
    2.4.2.1.               ProceedsFromRepaymentsOfLoansAndAdvancesToOtherEntities
    2.4.2.2.               PaymentsForLoansAndAdvancesToOtherEntities
    2.4.3.           Joint Ventures
    2.4.3.1.               PaymentsToAcquireJointVenture

    3.    NetCashProvidedByUsedInFinancingActivities
    3.1.        Equity Transactions
    3.1.1.            ProceedsFromIssuanceOfCommonStock
    3.1.2.            ProceedsFromIssuanceOfPreferredStock
    3.1.3.            ProceedsFromStockOptionsExercised
    3.1.4.            PaymentsForRepurchaseOfCommonStock
    3.1.5.            PaymentsForRepurchaseOfPreferredStock
    3.1.6.            ProceedsFromIssuanceOfOtherEquityInstruments
    3.1.7.            PaymentsForRepurchaseOfOtherEquityInstruments
    3.1.8.            ProceedsFromSaleOfTreasuryStock
    3.1.9.            PaymentsToAcquireTreasuryStock
    3.2.      Debt Transactions
    3.2.1.          ProceedsFromIssuanceOfDebt
    3.2.2.          RepaymentsOfDebt
    3.2.3.          ProceedsFromBorrowings
    3.2.4.          RepaymentsOfBorrowings
    3.3.       Dividends
    3.3.1.           DividendsPaid
    3.3.1.1.               DividendsPaidToNoncontrollingInterest
    3.3.1.2.               DividendsPaidToControllingInterest
    3.4.       Leases
    3.4.1.           PaymentsOfFinanceLeaseObligations
    3.4.2.           PaymentsOfOperatingLeaseLiabilities
    3.5.        Grants and Other Financing Sources
    3.5.1.            ProceedsFromGovernmentGrants

    4.    NetIncreaseDecreaseInCashAndCashEquivalents
    4.1.        CashAndCashEquivalentsPeriodIncreaseDecrease
    4.2.        EffectOfExchangeRateOnCashAndCashEquivalents
    4.3.        CashAndCashEquivalentsAtCarryingValue
    4.4.        CashAndCashEquivalentsBeginningOfPeriod

    </pre>




    Detailled View:
    <pre>
      Main Rules

      Post Rule (Cleanup)

    </pre>
    """
    prepivot_rule_tree = RuleGroup(
        prefix="CF_PREPIV",
        rules=[PrePivotDeduplicate(),]
    )

    preprocess_rule_tree = RuleGroup(prefix="CF_PRE",
                                     rules=[PreCorrectMixUpContinuingOperations()
                                     ])

    cf_netcash_exrate = RuleGroup(
        prefix="NETCASH_ExRATE",
        rules=[
            # Continuing Op
            # -------------
            CopyTagRule(original='NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
                        target='NetCashProvidedByUsedInOperatingActivities'),
            CopyTagRule(original='NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
                        target='NetCashProvidedByUsedInInvestingActivities'),
            CopyTagRule(original='NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
                        target='NetCashProvidedByUsedInFinancingActivities'),

            # Continuing Op EffectsOnExRate
            CopyTagRule(original='EffectOfExchangeRateOnCashContinuingOperations',
                        target='EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations'),

            # Continuing Op NetCashProvided
            SumUpRule(sum_tag='NetCashProvidedByUsedInContinuingOperations',
                      potential_summands=[
                          'NetCashProvidedByUsedInOperatingActivities',
                          'NetCashProvidedByUsedInInvestingActivities',
                          'NetCashProvidedByUsedInFinancingActivities',
                          'EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations'
                      ]),

            # Discontinued Op
            # -------------
            # Discontinued Op EffectsOnExRate
            CopyTagRule(original='EffectOfExchangeRateOnCashDiscontinuedOperations',
                        target='EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations'),

            # Discontinued Op NetCashProvided
            SumUpRule(sum_tag='NetCashProvidedByUsedInDiscontinuedOperations',
                      potential_summands=[
                          'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
                          'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations',
                          'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations',
                          'EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations'
                      ]),

            # ExRateEffect
            # ----------------------
            CopyTagRule(original='EffectOfExchangeRateOnCash',
                        target='EffectOfExchangeRateOnCashAndCashEquivalents'),
            CopyTagRule(original='EffectOfExchangeRateOnCashAndCashEquivalents',
                        target='EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents'),
            SumUpRule(sum_tag='EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations',
                      potential_summands=[
                          'EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents',
                          'EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsDisposalGroupIncludingDiscontinuedOperations'
                      ]),

            # Simplify name to EffectOfExchangeRateFinal
            CopyTagRule(original='EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations',
                        target='EffectOfExchangeRateFinal')

        ]
    )

    cf_increase_decrease = RuleGroup(
        prefix="INC_DEC",
        rules=[
            # Cash increase decrease Including ExRate Effect
            # ----------------------------------------------
            CopyTagRule(original='CashPeriodIncreaseDecrease',
                        target='CashAndCashEquivalentsPeriodIncreaseDecrease'),
            CopyTagRule(original='CashAndCashEquivalentsPeriodIncreaseDecrease',
                        target='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect'),

            # Simplify name to CashTotalPeriodIncreaseDecreaseIncludingExRateEffect
            CopyTagRule(original='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect',
                        target='CashTotalPeriodIncreaseDecreaseIncludingExRateEffect'),

            # Cash increase decrease Excluding ExRate Effect
            # ----------------------------------------------
            CopyTagRule(original='CashPeriodIncreaseDecreaseExcludingExchangeRateEffect',
                        target='CashAndCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect'),
            CopyTagRule(original='CashAndCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect',
                        target='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect'),

            # Simplify name to CashTotalPeriodIncreaseDecreaseExcludingExRateEffect
            CopyTagRule(original='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect',
                        target='CashTotalPeriodIncreaseDecreaseExcludingExRateEffect'),


        ])




        # todo: hier könnten noch missing summand/total rules definiert werden
        # für operating und discontinued
        # auch für CashTotalPeriodIncreaseDecreaseExcludingExRateEffect


    main_rule_tree = RuleGroup(prefix="CF",
                               rules=[
                                   cf_netcash_exrate,
                                   cf_increase_decrease,
                               ])


    post_rule_tree = RuleGroup(
        prefix="CF",
        rules=[
            PostSetToZero(tags=['NetCashProvidedByUsedInOperatingActivities', ]),
            PostSetToZero(tags=['NetCashProvidedByUsedInInvestingActivities', ]),
            PostSetToZero(tags=['NetCashProvidedByUsedInFinancingActivities', ]),
            PostSetToZero(tags=['EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations', ]),
            PostSetToZero(tags=['CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations', ]),
            PostSetToZero(tags=['CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations', ]),
            PostSetToZero(tags=['CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations', ]),
            PostSetToZero(tags=['EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations']),
            PostSetToZero(tags=['NetCashProvidedByUsedInDiscontinuedOperations']),
            PostSetToZero(tags=['EffectOfExchangeRateFinal']),

            MissingSumRule(sum_tag='NetCashProvidedByUsedInContinuingOperations',
                           summand_tags=['NetCashProvidedByUsedInOperatingActivities',
                                         'NetCashProvidedByUsedInInvestingActivities',
                                         'NetCashProvidedByUsedInFinancingActivities',
                                         'EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations']),

            MissingSumRule(sum_tag='NetCashProvidedByUsedInDiscontinuedOperations',
                           summand_tags=['CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
                                         'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations',
                                         'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations',
                                         'EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations']),

            MissingSumRule(sum_tag='CashTotalPeriodIncreaseDecreaseIncludingExRateEffect',
                           summand_tags=['CashTotalPeriodIncreaseDecreaseExcludingExRateEffect',
                                         'EffectOfExchangeRateFinal']),

            MissingSumRule(sum_tag='CashTotalPeriodIncreaseDecreaseIncludingExRateEffect',
                           summand_tags=['NetCashProvidedByUsedInContinuingOperations',
                                         'NetCashProvidedByUsedInDiscontinuedOperations',
                                         'EffectOfExchangeRateFinal']),




            # todo: hier werden noch final SUM up benötigt, für
            #  NetCashProvidedByUsedInContinuingOperations und NetCashProvidedByUsedInDiscontinuedOperations


        ])

    validation_rules: List[ValidationRule] = [
        SumValidationRule(identifier="NetCashContOp",
                          sum_tag='NetCashProvidedByUsedInContinuingOperations',
                          summands=['NetCashProvidedByUsedInOperatingActivities',
                                    'NetCashProvidedByUsedInFinancingActivities',
                                    'NetCashProvidedByUsedInInvestingActivities',
                                    'EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations']),
        SumValidationRule(identifier="NetCashDiscontOp",
                          sum_tag='NetCashProvidedByUsedInDiscontinuedOperations',
                          summands=['CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
                                    'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations',
                                    'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations',
                                    'EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations']),
        SumValidationRule(identifier="CashIncDecTotal",
                          sum_tag='CashTotalPeriodIncreaseDecreaseIncludingExRateEffect',
                          summands=['NetCashProvidedByUsedInContinuingOperations',
                                    'NetCashProvidedByUsedInDiscontinuedOperations',
                                    'EffectOfExchangeRateFinal']),
        SumValidationRule(identifier="BaseOpAct",
                          sum_tag='NetCashProvidedByUsedInOperatingActivities',
                          summands=['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
                                    'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations']),

        SumValidationRule(identifier="BaseFinAct",
                          sum_tag='NetCashProvidedByUsedInFinancingActivities',
                          summands=['NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
                                    'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations']),

        SumValidationRule(identifier="BaseInvAct",
                          sum_tag='NetCashProvidedByUsedInInvestingActivities',
                          summands=['NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
                                    'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations']),
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = [
        'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInOperatingActivities',
        'NetCashProvidedByUsedInFinancingActivities',
        'NetCashProvidedByUsedInInvestingActivities',
        'EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations',
        'NetCashProvidedByUsedInContinuingOperations',
        'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
        'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations',
        'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations',
        'EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations',
        'NetCashProvidedByUsedInDiscontinuedOperations',
        'CashTotalPeriodIncreaseDecreaseExcludingExRateEffect',
        'EffectOfExchangeRateFinal',
        'CashTotalPeriodIncreaseDecreaseIncludingExRateEffect'
    ]

    # used to evaluate if a report is the main cashflow report
    # inside a report, there can be several tables (different report nr)
    # which stmt value is CF.
    # however, we might be only interested in the "major" CF report. Usually this is the
    # one which has the least nan in the following columns
    main_statement_tags = [
        'NetCashProvidedByUsedInOperatingActivities',
        'NetCashProvidedByUsedInFinancingActivities',
        'NetCashProvidedByUsedInInvestingActivities',
        'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInContinuingOperations',
        'NetCashProvidedByUsedInDiscontinuedOperations',
        'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect',
        'CashAndCashEquivalentsPeriodIncreaseDecrease',
        'CashPeriodIncreaseDecrease',
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
