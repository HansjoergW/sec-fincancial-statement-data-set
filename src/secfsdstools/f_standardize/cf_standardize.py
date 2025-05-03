# pylint: disable=C0302
"""Contains the definitions to standardize incaome statements."""
from typing import List, Optional, Set

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_prepivot_rules import PrePivotCorrectSign, PrePivotDeduplicate, PrePivotMaxQtrs
from secfsdstools.f_standardize.base_rule_framework import PrePivotRule, Rule, RuleGroup
from secfsdstools.f_standardize.base_rules import (
    CopyTagRule,
    MissingSummandRule,
    MissingSumRule,
    PostCopyToFirstSummand,
    PostSetToZero,
    SumUpRule,
)
from secfsdstools.f_standardize.base_validation_rules import IsSetValidationRule, SumValidationRule, ValidationRule
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
    Normally, NetCashProvidedByUsedInXXXActivities is the sum of
    NetCashProvidedByUsedInXXXActivitiesContinuingOperations and
    CashProvidedByUsedInXXXgActivitiesDiscontinuedOperations

    However, often when CashProvidedByUsedInXXXgActivitiesDiscontinuedOperations is present,
    NetCashProvidedByUsedInXXXActivities is used to tag the continuingOperations instead of
    NetCashProvidedByUsedInXXXActivitiesContinuingOperations.

    This results in a wrong summation and therefore validation.

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

        # how to detect..
        # Summing up the NetCashProvidedByUsedInOperatingActivities values and adding
        # the value of EffectOfExchangeRateFinal gives the expected
        # CashPeriodIncreaseDecrease value.

        expected_sum = data_df[self.sum_tags].sum(axis=1) + data_df['EffectOfExchangeRateFinal']

        # we sum up the values for the discontinued tags, and create a pathfilter if they are all 0
        sum_disc = data_df[self.disc_tags].sum(axis=1)
        mask_sum_disc_not_zero = ~(sum_disc == 0)

        # now, we compare the expected CashPeriodIncDec value with the actual value the tag
        # the difference should be 0.0
        diff = data_df['CashPeriodIncreaseDecreaseIncludingExRateEffectFinal'] - expected_sum

        # So if the difference matches the sum of the disc operations, we know that the
        # Discontinued Value is not in the appropriate SumTag.

        # Explanation:
        # it should be that
        #   + NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
        #   + CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations
        #   = NetCashProvidedByUsedInOperatingActivities (expected)
        #
        #  However, if the tag NetCashProvidedByUsedInOperatingActivities was used to tag the
        #  actual value for NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
        #  the value is off by the value of
        #  CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations

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
            data_df.loc[mask, self.sum_tags[idx]] = (
                    data_df[self.cont_tags[idx]] + data_df[self.disc_tags[idx]])
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
          CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations
          NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
              DepreciationDepletionAndAmortization
              DeferredIncomeTaxExpenseBenefit
              ShareBasedCompensation
              IncreaseDecreaseInAccountsPayable
              IncreaseDecreaseInAccruedLiabilities
              InterestPaidNet
              IncomeTaxesPaidNet

        NetCashProvidedByUsedInInvestingActivities
            CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations
            NetCashProvidedByUsedInInvestingActivitiesContinuingOperations
              PaymentsToAcquirePropertyPlantAndEquipment
              ProceedsFromSaleOfPropertyPlantAndEquipment
              PaymentsToAcquireInvestments
              ProceedsFromSaleOfInvestments
              PaymentsToAcquireBusinessesNetOfCashAcquired
              ProceedsFromDivestitureOfBusinessesNetOfCashDivested
              PaymentsToAcquireIntangibleAssets
              ProceedsFromSaleOfIntangibleAssets

        NetCashProvidedByUsedInFinancingActivities
            CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations
            NetCashProvidedByUsedInFinancingActivitiesContinuingOperations
              ProceedsFromIssuanceOfCommonStock
              ProceedsFromStockOptionsExercised
              PaymentsForRepurchaseOfCommonStock
              ProceedsFromIssuanceOfDebt
              RepaymentsOfDebt
              PaymentsOfDividends


        EffectOfExchangeRateFinal
        CashPeriodIncreaseDecreaseIncludingExRateEffectFinal

        CashAndCashEquivalentsEndOfPeriod
    </pre>

    Rule overview:
    <pre>
          + NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
          + CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations <- CashProvidedByUsedInDiscontinuedOperationsOperatingActivities
          --------
      + NetCashProvidedByUsedInOperatingActivities

          + NetCashProvidedByUsedInInvestingActivitiesContinuingOperations
          + CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations <- CashProvidedByUsedInDiscontinuedOperationsInvestingActivities
          --------
      + NetCashProvidedByUsedInInvestingActivities

          + NetCashProvidedByUsedInInvestingActivitiesContinuingOperations
          + CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations <- CashProvidedByUsedInDiscontinuedOperationsFinancingActivities
          --------
      + NetCashProvidedByUsedInInvestingActivities


                  Prio 1
                           <- EffectOfExchangeRateOnCash
                       <- EffectOfExchangeRateOnCashAndCashEquivalents
                  + EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents
                  + EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsDisposalGroupIncludingDiscontinuedOperations
                  --------
                  EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations
                 <-
              EffectOfExchangeRateFinal


                  Prio 2
                  + EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations <- EffectOfExchangeRateOnCashContinuingOperations
                  + EffectOfExchangeRateOnCashAndCashEquivalentsDiscontinuedOperations <- EffectOfExchangeRateOnCashDiscontinuedOperations
                  --------
              EffectOfExchangeRateFinal


      + EffectOfExchangeRateFinal
      ---------------------------

                   <- CashPeriodIncreaseDecrease
             <- CashAndCashEquivalentsPeriodIncreaseDecrease
        <- CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect
      CashPeriodIncreaseDecreaseIncludingExRateEffectFinal
      ==================

                                       <- CashEndOfPeriod
                                  <- CashAndDueFromBanksEndOfPeriod
                             <- CashAndCashEquivalentsAtCarryingValueEndOfPeriod
                        <- CashAndCashEquivalentsAtCarryingValueIncludingDiscontinuedOperationsEndOfPeriod
                   <- CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsEndOfPeriod
              <- CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsDisposalGroupIncludingDiscontinuedOperationsEndOfPeriod
        <- CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperationsEndOfPeriod
      CashAndCashEquivalentsEndOfPeriod


      Details of Operating Activities

                  + AmortizationOfIntangibleAssets
                  + AmortizationOfDeferredCharges
                  + AmortizationOfFinancingCosts
                  -------------
                + Amortization
                + Depreciation
                --------------
                + DepreciationAndAmortization
                + Depletion
                ----------
            DepreciationDepletionAndAmortization

            DeferredIncomeTaxExpenseBenefit
            ShareBasedCompensation
            IncreaseDecreaseInAccountsPayable
            IncreaseDecreaseInAccruedLiabilities
            InterestPaidNet
            IncomeTaxesPaidNet


      Details of Investing activities

                + ProceedsFromSaleOfAvailableForSaleSecurities
                + ProceedsFromSaleOfTradingSecurities
                + ProceedsFromSaleOfEquitySecurities
                + ProceedsFromSaleOfDebtSecurities
                + ProceedsFromSaleAndMaturityOfOtherInvestments
                + ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities
                + ProceedsFromSaleOfInvestmentsInAffiliates
                + ProceedsFromSaleOfHeldToMaturitySecurities
                --------------------------------------------
            ProceedsFromSaleOfInvestments

            PaymentsToAcquirePropertyPlantAndEquipment
            ProceedsFromSaleOfPropertyPlantAndEquipment
            PaymentsToAcquireInvestments
            PaymentsToAcquireBusinessesNetOfCashAcquired
            ProceedsFromDivestitureOfBusinessesNetOfCashDivested
            PaymentsToAcquireIntangibleAssets
            ProceedsFromSaleOfIntangibleAssets


      Details of Financing activities

                  + PaymentsOfDividendsCommonStock
                  + PaymentsOfDividendsPreferredStockAndPreferenceStock
                  + PaymentsOfDividendsMinorityInterest
                  -------------
             PaymentsOfDividends

             ProceedsFromIssuanceOfCommonStock
             ProceedsFromStockOptionsExercised
             PaymentsForRepurchaseOfCommonStock
             ProceedsFromIssuanceOfDebt
             RepaymentsOfDebt

     Post Rules

        Calculate missing "Operating" tags

        + NetCashProvidedByUsedInOperatingActivities (if present)
        - NetCashProvidedByUsedInOperatingActivitiesContinuingOperations (if present)
        -------
        = CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations (if not present)


        + NetCashProvidedByUsedInOperatingActivities (if present)
        - CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations (if present)
        -------
        = NetCashProvidedByUsedInOperatingActivitiesContinuingOperations (if not present)


        if only NetCashProvidedByUsedInOperatingActivities is set:
          NetCashProvidedByUsedInOperatingActivitiesContinuingOperations = NetCashProvidedByUsedInOperatingActivities
          CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations = 0


        Same as above for Financing and Investing triples

        Set to Zero if still not set:
           NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
           NetCashProvidedByUsedInInvestingActivitiesContinuingOperations
           NetCashProvidedByUsedInFinancingActivitiesContinuingOperations
           NetCashProvidedByUsedInOperatingActivities
           NetCashProvidedByUsedInInvestingActivities
           NetCashProvidedByUsedInFinancingActivities
           CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations
           CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations
           CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations

           EffectOfExchangeRateFinal

        Set CashPeriodIncreaseDecreaseIncludingExRateEffectFinal (if not set yet)
            + NetCashProvidedByUsedInOperatingActivities
            + NetCashProvidedByUsedInInvestingActivities
            + NetCashProvidedByUsedInFinancingActivities
            + EffectOfExchangeRateFinal
            ------------------
            = CashPeriodIncreaseDecreaseIncludingExRateEffectFinal

        Fix mixed up usage of NetCashProvidedByUsed...Activities/-ContinuingOperations
    </pre>

    """
    prepivot_rule_tree = RuleGroup(
        prefix="CF_PREPIV",
        rules=[PrePivotDeduplicate(),  # remove duplicates
               PrePivotMaxQtrs(max_qtrs=4),  # only keep entries with qtrs <= 4
               PrePivotCorrectSign(
                   tag_list=inflow_tags,  # make sure these tags have a positive value
                   is_positive=True
               ),
               PrePivotCorrectSign(
                   tag_list=outflow_tags,  # make sure these tags have a negative value
                   is_positive=False
               ),
               PrePivotCashAtEndOfPeriod()  # prepare CashAtEndOfPeriod
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
                target='CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsDisposalGroupIncludingDiscontinuedOperationsEndOfPeriod'),
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
        'DeferredIncomeTaxExpenseBenefit',
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

    def __init__(self,
                 prepivot_rule_tree: Optional[RuleGroup] = None,
                 pre_rule_tree: Optional[RuleGroup] = None,
                 main_rule_tree: Optional[RuleGroup] = None,
                 post_rule_tree: Optional[RuleGroup] = None,
                 validation_rules: Optional[List[ValidationRule]] = None,
                 final_tags: Optional[List[str]] = None,
                 main_statement_tags: Optional[List[str]] = None,

                 filter_for_main_statement: bool = True,
                 main_iterations: int = 1,
                 invert_negated: bool = True,
                 additional_final_sub_fields: Optional[List[str]] = None,
                 additional_final_tags: Optional[List[str]] = None):
        """
        Initialize the CashFlow Standardizer.

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
                    Default is 1 for CashFlow.
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
