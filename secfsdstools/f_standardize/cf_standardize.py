"""Contains the definitions to standardize incaome statements."""
from typing import List, Set

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_prepivot_rules import (PrePivotDeduplicate,
                                                            PrePivotCorrectSign, PrePivotMaxQtrs)
from secfsdstools.f_standardize.base_rule_framework import RuleGroup, Rule, PrePivotRule
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule, PostSetToZero, \
    MissingSummandRule, PostCopyToFirstSummand, MissingSumRule
from secfsdstools.f_standardize.base_validation_rules import ValidationRule, SumValidationRule, \
    IsSetValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer

# list of tags which indicate an inflow of money and therefore should have a positive value
inflow_tags = [
    'ProceedsFromDivestitureOfBusinessesNetOfCashDivested',
    'ProceedsFromIssuanceOfCommonStock',
    'ProceedsFromIssuanceOfDebt',
    'ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities',
    'ProceedsFromSaleAndMaturityOfOtherInvestments',
    'ProceedsFromSaleOfAvailableForSaleSecurities',
    'ProceedsFromSaleOfHeldToMaturitySecurities',
    'ProceedsFromSaleOfIntangibleAssets',
    'ProceedsFromSaleOfPropertyPlantAndEquipment',
    'ProceedsFromStockOptionsExercised',
    'AmortizationOfDeferredCharges',
    'AmortizationOfFinancingCosts',
    'AmortizationOfIntangibleAssets',
    'Depletion',
    'Depreciation',
    'DepreciationAndAmortization',
    'DepreciationDepletionAndAmortization',
]

# list of tags which indicate an outflow of money and therefore should have a positive value
outflow_tags = [
    'PaymentsForRepurchaseOfCommonStock',
    'PaymentsOfDividends',
    'PaymentsOfDividendsCommonStock',
    'PaymentsOfDividendsMinorityInterest',
    'PaymentsOfDividendsPreferredStockAndPreferenceStock',
    'PaymentsToAcquireBusinessesNetOfCashAcquired',
    'PaymentsToAcquireIntangibleAssets',
    'PaymentsToAcquireInvestments',
    'PaymentsToAcquirePropertyPlantAndEquipment',
    'RepaymentsOfDebt',
]


class PrePivotCashAtEndOfPeriod(PrePivotRule):
    """
    The cash at the beginning and end of a period always has qtrs=0, since this is
    "point-in-time" value and not a period value.

    However, when we pivot, we need to use the qtrs as key and therefore,
    this value is lost.

    Therefore, for all "cash" indicators:
    - CashAndCashEquivalentsAtCarryingValue
    - CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents
    - CashAndCashEquivalentsAtCarryingValueIncludingDiscontinuedOperations

    we add copied entries for the qtrs that are present for that adsh number.
    """

    # pylint: disable=C0301
    def __init__(self):
        super().__init__("CashEndOfPeriod")
        self.cash_tags = [
            'Cash',
            'CashAndDueFromBanks',
            'CashAndCashEquivalentsAtCarryingValue',
            'CashAndCashEquivalentsAtCarryingValueIncludingDiscontinuedOperations',
            'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents',
            'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsDisposalGroupIncludingDiscontinuedOperations',
            'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations',
        ]

    def get_input_tags(self) -> Set[str]:
        return set(self.cash_tags)

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            data_df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        return data_df.tag.isin(self.cash_tags) & (data_df.qtrs == 0)

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> pd.DataFrame:
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """
        # dataframe with unique entries of qdsh and qtrs
        adsh_qtrs_df = data_df[['adsh', 'qtrs']].drop_duplicates()

        relevant_entries_df = data_df[mask]

        added_dfs: List[pd.DataFrame] = []
        # copy entries for qtrs 0...4
        for qtrs in range(5):
            # define which adshs have entries with the current qtrs loop-variable
            adshs_with_qtrs = adsh_qtrs_df[adsh_qtrs_df.qtrs == qtrs].adsh.tolist()

            # create new entries only for the adshs that have values in the current qtr
            new_df = relevant_entries_df[relevant_entries_df.adsh.isin(adshs_with_qtrs)].copy()
            new_df['qtrs'] = qtrs
            new_df['tag'] = new_df.tag + "EndOfPeriod"
            added_dfs.append(new_df)

        return pd.concat([data_df] + added_dfs)

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return (f"Adds copies of rows for {self.cash_tags} with the 'qtrs' set to the values that"
                "are present for the corresponding 'adsh' and extending the tag name with "
                " 'EndOfPeriod'. ")


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

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> pd.DataFrame:
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """

        data_df.loc[mask, self.opact] = data_df[self.opcont]
        data_df.loc[mask, self.opcont] = None
        return data_df

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


class PostFixMixedContinuingWithSum(Rule):
    """
    Normally, when NetCashProvidedByUsedInXXXActivities are the sum of
    NetCashProvidedByUsedInXXXActivitiesContinuingOperations and
    CashProvidedByUsedInXXXgActivitiesDiscontinuedOperations

    However, often when CashProvidedByUsedInXXXgActivitiesDiscontinuedOperations is present,
    NetCashProvidedByUsedInXXXActivities is used to tag the continuingOperations instead of
    NetCashProvidedByUsedInXXXActivitiesContinuingOperations.

    This results in wrong summation and therefore validation.

    This rule fixes these entries.
    """

    def __init__(self):
        self.disc_tags = ['CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
                          'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations',
                          'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations']
        self.cont_tags = ['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
                          'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
                          'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations']
        self.sum_tags = ['NetCashProvidedByUsedInOperatingActivities',
                         'NetCashProvidedByUsedInFinancingActivities',
                         'NetCashProvidedByUsedInInvestingActivities']

    def get_target_tags(self) -> List[str]:
        """
        returns a list of tags that could be affected/changed by this rule

        Returns:
            List[str]: list with affected tags/columns
        """
        return self.sum_tags + self.cont_tags

    def get_input_tags(self) -> Set[str]:
        """
        return all tags that the rules within this group need.

        Returns:
            Set[str]: set of all input tags that are used by rules in this group
        """
        return set(self.disc_tags + self.cont_tags + self.sum_tags)

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.

        Args:
            data_df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        expected_sum = data_df[self.sum_tags].sum(axis=1) + data_df['EffectOfExchangeRateFinal']
        sum_disc = data_df[self.disc_tags].sum(axis=1)
        mask_sum_disc_not_zero = ~(sum_disc == 0)

        diff = data_df['CashPeriodIncreaseDecreaseIncludingExRateEffectFinal'] - expected_sum

        mask: pa.typing.Series[bool] = mask_sum_disc_not_zero & (diff == sum_disc)
        return mask

    def apply(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> pd.DataFrame:
        """
        apply the rule on the provided dataframe. the rows, on which the rule has to be applied
        is defined by the provide mask Series.

        Important, the rules have to be applied "in-place", so no new dataframe is produced.

        Args:
            df: dataframe on which the rule has to be applied
            mask: a Series marking the rows in the dataframe on which the rule has to be applied
        Returns:
            pd.DataFrame: make the process chainable
        """
        for idx in range(3):
            data_df.loc[mask, self.cont_tags[idx]] = data_df[self.sum_tags[idx]]
            data_df.loc[mask, self.sum_tags[idx]] = data_df[self.cont_tags[idx]] + data_df[
                self.disc_tags[idx]]
        return data_df

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return ("Tries to find and correct cases when discontinued operations are reported and "
                "the 'Sum' tags (e.g.'NetCashProvidedByUsedIn...Activities') were used to"
                " report the continuing operation instead using the of the "
                " '...ContinuingOperations' tags. "
                "(e.g. 'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations').")

# pylint: disable=C0301

class CashFlowStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create CashFlow statements that are comparable,
    meaning that they have the same tags.

    At the end, the standardized CF contains the following columns

    <pre>

    Final Tags
        NetCashProvidedByUsedInOperatingActivities
          DepreciationDepletionAndAmortization                 ok Hierarchy
          DeferredIncomeTaxExpenseBenefit                      ok direct
          ShareBasedCompensation                               ok direct
          IncreaseDecreaseInAccountsPayable                    ok direct
          IncreaseDecreaseInAccruedLiabilities                 ok direct
          InterestPaidNet                                      ok direct
          IncomeTaxesPaidNet                                   ok direct

        NetCashProvidedByUsedInInvestingActivities
          PaymentsToAcquirePropertyPlantAndEquipment           ok direct
          ProceedsFromSaleOfPropertyPlantAndEquipment          ok direct
          PaymentsToAcquireInvestments                         ok direct
          ProceedsFromSaleOfInvestments                        ok Hierarchy
          PaymentsToAcquireBusinessesNetOfCashAcquired         ok direct
          ProceedsFromDivestitureOfBusinessesNetOfCashDivested ok direct
          PaymentsToAcquireIntangibleAssets                    ok direct
          ProceedsFromSaleOfIntangibleAssets                   ok direct

        NetCashProvidedByUsedInFinancingActivities
          ProceedsFromIssuanceOfCommonStock                    ok direct
          ProceedsFromStockOptionsExercised                    ok direct
          PaymentsForRepurchaseOfCommonStock                   ok direct
          ProceedsFromIssuanceOfDebt                           ok direct
          RepaymentsOfDebt                                     ok direct
          PaymentsOfDividends                                  ok Hierarchy


        NetIncreaseDecreaseInCashAndCashEquivalents
        4.1. CashAndCashEquivalentsPeriodIncreaseDecrease
        4.2. EffectOfExchangeRateOnCashAndCashEquivalents

    </pre>

    """
    prepivot_rule_tree = RuleGroup(
        prefix="CF_PREPIV",
        rules=[PrePivotDeduplicate(), # remove duplicates
               PrePivotMaxQtrs(max_qtrs=4), # only keep entries with qtrs <= 4
               PrePivotCorrectSign(
                   tag_list=inflow_tags, # make sure these tags have a positive value
                   is_positive=True
               ),
               PrePivotCorrectSign(
                   tag_list=outflow_tags, # make sure these tags have a negative value
                   is_positive=False
               ),
               PrePivotCashAtEndOfPeriod() # prepare CashAtEndOfPeriod
               ]
    )

    preprocess_rule_tree = (
        RuleGroup(prefix="CF_PRE",
                  rules=[
                      # fix wrong usage of NetCashProvidedByUsedInContinuingOperations
                      PreCorrectMixUpContinuingOperations(),
                  ]))

    cf_netcash = RuleGroup(
        prefix="NETCASH",
        rules=[

            # found in older reports, very rarely used (around 100 times)
            CopyTagRule(original='CashProvidedByUsedInDiscontinuedOperationsOperatingActivities',
                        target='CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations'),

            CopyTagRule(original='CashProvidedByUsedInDiscontinuedOperationsInvestingActivities',
                        target='CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations'),

            CopyTagRule(original='CashProvidedByUsedInDiscontinuedOperationsFinancingActivities',
                        target='CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations'),

            # Calculate SumTags for Continuing and Discontinued
            SumUpRule(sum_tag='NetCashProvidedByUsedInOperatingActivities',
                      potential_summands=[
                          'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
                          'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
                      ]),
            SumUpRule(sum_tag='NetCashProvidedByUsedInFinancingActivities',
                      potential_summands=[
                          'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
                          'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations',
                      ]),
            SumUpRule(sum_tag='NetCashProvidedByUsedInInvestingActivities',
                      potential_summands=[
                          'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
                          'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations',
                      ]),
        ]
    )

    cf_eff_ex_rate = RuleGroup(
        prefix="EFF_EXRATE",
        rules=[
            # ExRateEffect
            # ----------------------
            CopyTagRule(original='EffectOfExchangeRateOnCash',
                        target='EffectOfExchangeRateOnCashAndCashEquivalents'),
            CopyTagRule(original='EffectOfExchangeRateOnCashAndCashEquivalents',
                        target='EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents'),
            SumUpRule(
                sum_tag='EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations',
                potential_summands=[
                    'EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents',
                    'EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsDisposalGroupIncludingDiscontinuedOperations'
                ]),

            # Simplify name to EffectOfExchangeRateFinal
            CopyTagRule(
                original='EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations',
                target='EffectOfExchangeRateFinal'),

            # Sometimes also EffectOfExchangeRateOnCashContinuingOperations are used
            # Continuing Op EffectsOnExRate
            CopyTagRule(original='EffectOfExchangeRateOnCashContinuingOperations',
                        target='EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations'),

            # Discontinued Op EffectsOnExRate
            CopyTagRule(original='EffectOfExchangeRateOnCashDiscontinuedOperations',
                        target='EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations'),

            SumUpRule(
                sum_tag='EffectOfExchangeRateFinal',
                potential_summands=[
                    'EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations',
                    'EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations'
                ]
            )
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
            CopyTagRule(
                original='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect',
                target='CashPeriodIncreaseDecreaseIncludingExRateEffectFinal'),
        ])

    cf_end_of_period = RuleGroup(
        prefix="EOP",
        rules=[
            CopyTagRule(
                original='CashEndOfPeriod',
                target='CashAndDueFromBanksEndOfPeriod'),
            CopyTagRule(
                original='CashAndDueFromBanksEndOfPeriod',
                target='CashAndCashEquivalentsAtCarryingValueEndOfPeriod'),
            CopyTagRule(
                original='CashAndCashEquivalentsAtCarryingValueEndOfPeriod',
                target='CashAndCashEquivalentsAtCarryingValueIncludingDiscontinuedOperationsEndOfPeriod'),
            CopyTagRule(
                original='CashAndCashEquivalentsAtCarryingValueIncludingDiscontinuedOperationsEndOfPeriod',
                target='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsEndOfPeriod'),
            CopyTagRule(
                original='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsEndOfPeriod',
                target='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperationsEndOfPeriod'),
            CopyTagRule(
                original='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsDisposalGroupIncludingDiscontinuedOperationsEndOfPeriod',
                target='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperationsEndOfPeriod'),
            CopyTagRule(
                original='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperationsEndOfPeriod',
                target='CashAndCashEquivalentsEndOfPeriod'),
        ]
    )

    # DepreciationDepletionAndAmortization
    cf_depr_depl_amort = RuleGroup(
        # 1. DepreciationDepletionAndAmortization
        # 1.1 DepreciationAndAmortization
        # 1.1.1 Depreciation
        # 1.1.2 Amortization
        # 1.1.2.1 AmortizationOfIntangibleAssets
        # 1.1.2.2 AmortizationOfDeferredCharges
        # 1.1.2.3 AmortizationOfFinancingCosts
        # 1.2 Depletion

        prefix="DeprDeplAmort",
        rules=[
            SumUpRule(
                sum_tag='Amortization',
                potential_summands=[
                    'AmortizationOfIntangibleAssets',
                    'AmortizationOfDeferredCharges',
                    'AmortizationOfFinancingCosts',
                ]),
            SumUpRule(
                sum_tag='DepreciationAndAmortization',
                potential_summands=[
                    'Amortization',
                    'Depreciation',
                ]),
            SumUpRule(
                sum_tag='DepreciationDepletionAndAmortization',
                potential_summands=[
                    'DepreciationAndAmortization',
                    'Depletion',
                ]),
        ]
    )

    cf_proceeds_sale_invest = RuleGroup(
        prefix="ProSalesInvest",
        rules=[
            SumUpRule(
                sum_tag="ProceedsFromSaleOfInvestments",
                potential_summands=[
                    'ProceedsFromSaleOfAvailableForSaleSecurities',
                    'ProceedsFromSaleOfTradingSecurities',
                    'ProceedsFromSaleOfEquitySecurities',
                    'ProceedsFromSaleOfDebtSecurities',
                    'ProceedsFromSaleAndMaturityOfOtherInvestments',
                    'ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities',
                    'ProceedsFromSaleOfInvestmentsInAffiliates',
                    'ProceedsFromSaleOfHeldToMaturitySecurities',
                ]
            )
        ]
    )

    cf_payments_dividends = RuleGroup(
        prefix="PayDividends",
        rules=[
            SumUpRule(
                sum_tag="PaymentsOfDividends",
                potential_summands=[
                    'PaymentsOfDividendsCommonStock',
                    'PaymentsOfDividendsPreferredStockAndPreferenceStock',
                    'PaymentsOfDividendsMinorityInterest',
                ]
            )
        ]
    )

    main_rule_tree = RuleGroup(prefix="CF",
                               rules=[
                                   cf_netcash,
                                   cf_eff_ex_rate,
                                   cf_increase_decrease,
                                   cf_end_of_period,
                                   cf_depr_depl_amort,
                                   cf_proceeds_sale_invest,
                                   cf_payments_dividends,
                               ])

    post_rule_tree = RuleGroup(
        prefix="CF",
        rules=[
            # final completion of NetCash-Tags
            MissingSummandRule(sum_tag='NetCashProvidedByUsedInOperatingActivities',
                               existing_summands_tags=[
                                   'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'],
                               missing_summand_tag='CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations'),

            MissingSummandRule(sum_tag='NetCashProvidedByUsedInOperatingActivities',
                               existing_summands_tags=[
                                   'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations'],
                               missing_summand_tag='NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'),

            PostCopyToFirstSummand(sum_tag='NetCashProvidedByUsedInOperatingActivities',
                                   first_summand='NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
                                   other_summands=[
                                       'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations']),

            MissingSummandRule(sum_tag='NetCashProvidedByUsedInFinancingActivities',
                               existing_summands_tags=[
                                   'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations'],
                               missing_summand_tag='CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations'),

            MissingSummandRule(sum_tag='NetCashProvidedByUsedInFinancingActivities',
                               existing_summands_tags=[
                                   'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations'],
                               missing_summand_tag='NetCashProvidedByUsedInFinancingActivitiesContinuingOperations'),

            PostCopyToFirstSummand(sum_tag='NetCashProvidedByUsedInFinancingActivities',
                                   first_summand='NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
                                   other_summands=[
                                       'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations']),

            MissingSummandRule(sum_tag='NetCashProvidedByUsedInInvestingActivities',
                               existing_summands_tags=[
                                   'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations'],
                               missing_summand_tag='CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations'),

            MissingSummandRule(sum_tag='NetCashProvidedByUsedInInvestingActivities',
                               existing_summands_tags=[
                                   'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations'],
                               missing_summand_tag='NetCashProvidedByUsedInInvestingActivitiesContinuingOperations'),

            PostCopyToFirstSummand(sum_tag='NetCashProvidedByUsedInInvestingActivities',
                                   first_summand='NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
                                   other_summands=[
                                       'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations']),

            # Setting values to zero of tags which were not set
            PostSetToZero(
                tags=['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', ]),
            PostSetToZero(
                tags=['NetCashProvidedByUsedInInvestingActivitiesContinuingOperations', ]),
            PostSetToZero(
                tags=['NetCashProvidedByUsedInFinancingActivitiesContinuingOperations', ]),
            PostSetToZero(tags=['NetCashProvidedByUsedInOperatingActivities', ]),
            PostSetToZero(tags=['NetCashProvidedByUsedInInvestingActivities', ]),
            PostSetToZero(tags=['NetCashProvidedByUsedInFinancingActivities', ]),
            PostSetToZero(tags=['CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations', ]),
            PostSetToZero(tags=['CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations', ]),
            PostSetToZero(tags=['CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations', ]),

            PostSetToZero(tags=['EffectOfExchangeRateFinal']),

            # Final Summation of CashPeriodIncreaseDecreaseIncludingExRateEffectFinal
            MissingSumRule(sum_tag='CashPeriodIncreaseDecreaseIncludingExRateEffectFinal',
                           summand_tags=['NetCashProvidedByUsedInOperatingActivities',
                                         'NetCashProvidedByUsedInInvestingActivities',
                                         'NetCashProvidedByUsedInFinancingActivities',
                                         'EffectOfExchangeRateFinal']),

            PostFixMixedContinuingWithSum(),

        ])

    validation_rules: List[ValidationRule] = [
        SumValidationRule(identifier="BaseOpAct",
                          sum_tag='NetCashProvidedByUsedInOperatingActivities',
                          summands=[
                              'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
                              'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations']),

        SumValidationRule(identifier="BaseFinAct",
                          sum_tag='NetCashProvidedByUsedInFinancingActivities',
                          summands=[
                              'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
                              'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations']),

        SumValidationRule(identifier="BaseInvAct",
                          sum_tag='NetCashProvidedByUsedInInvestingActivities',
                          summands=[
                              'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
                              'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations']),

        SumValidationRule(identifier="NetCashContOp",
                          sum_tag='CashPeriodIncreaseDecreaseIncludingExRateEffectFinal',
                          summands=['NetCashProvidedByUsedInOperatingActivities',
                                    'NetCashProvidedByUsedInFinancingActivities',
                                    'NetCashProvidedByUsedInInvestingActivities',
                                    'EffectOfExchangeRateFinal']),

        IsSetValidationRule(identifier="CashEoP",
                            tag='CashAndCashEquivalentsEndOfPeriod')
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = [
        'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
        'NetCashProvidedByUsedInOperatingActivities',
        'NetCashProvidedByUsedInFinancingActivities',
        'NetCashProvidedByUsedInInvestingActivities',
        'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
        'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations',
        'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations',
        'EffectOfExchangeRateFinal',
        'CashPeriodIncreaseDecreaseIncludingExRateEffectFinal',
        'CashAndCashEquivalentsEndOfPeriod',

        # Details in Operating activities
        'DepreciationDepletionAndAmortization',
        'DeferredIncomeTaxExpenseBenefit,'
        'ShareBasedCompensation',
        'IncreaseDecreaseInAccountsPayable',
        'IncreaseDecreaseInAccruedLiabilities',
        'InterestPaidNet',
        'IncomeTaxesPaidNet',

        # Details of Investing activities
        'PaymentsToAcquirePropertyPlantAndEquipment',
        'ProceedsFromSaleOfPropertyPlantAndEquipment',
        'PaymentsToAcquireInvestments',
        'ProceedsFromSaleOfInvestments',
        'PaymentsToAcquireBusinessesNetOfCashAcquired',
        'ProceedsFromDivestitureOfBusinessesNetOfCashDivested',
        'PaymentsToAcquireIntangibleAssets',
        'ProceedsFromSaleOfIntangibleAssets',

        # Details of Financing activities
        'ProceedsFromIssuanceOfCommonStock',
        'ProceedsFromStockOptionsExercised',
        'PaymentsForRepurchaseOfCommonStock',
        'ProceedsFromIssuanceOfDebt',
        'RepaymentsOfDebt',
        'PaymentsOfDividends',

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

    def __init__(self, filter_for_main_statement: bool = True, iterations: int = 1):
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
