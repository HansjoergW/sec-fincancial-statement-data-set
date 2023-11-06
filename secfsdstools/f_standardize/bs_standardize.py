from abc import ABC
from abc import abstractmethod
from typing import List, Optional, Set

import numpy as np
import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.rule_framework import Rule, RuleGroup, RuleEntity
from secfsdstools.f_standardize.base_rules import CopyTagRule



class MissingSumRule(Rule):
    """ creates the sum in the sum_name column if all summand_names columnes have a value"""

    def __init__(self, sum_name: str, summand_names: List[str]):
        self.sum_name = sum_name
        self.summand_names = summand_names

    def get_target_tags(self) -> List[str]:
        return [self.sum_name]

    def get_input_tags(self) -> Set[str]:
        result = {self.sum_name}
        result.update(self.summand_names)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        # sum_name has to be not set and all summand_names have to be set
        mask = df[self.sum_name].isna()
        for summand_name in self.summand_names:
            mask = mask & ~df[summand_name].isna()

        return mask

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        df.loc[mask, self.sum_name] = df[self.summand_names].sum(axis=1)

    def get_description(self) -> str:
        return ""

class MissingSummandRule(Rule):

    def __init__(self, sum_name: str, missing_summand: str, existing_summands: List[str]):
        self.sum_name = sum_name
        self.missing_summand = missing_summand
        self.existing_summands = existing_summands

    def get_target_tags(self) -> List[str]:
        return [self.missing_summand]

    def get_input_tags(self) -> Set[str]:
        result = {self.sum_name, self.missing_summand}
        result.update(self.existing_summands)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        # sum and other_summands must be set, missing_summand must not be set
        mask = ~df[self.sum_name].isna()
        for summand_name in self.existing_summands:
            mask = mask & ~df[summand_name].isna()
        mask = mask & df[self.missing_summand].isna()
        return mask

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        df.loc[mask, self.missing_summand] = \
            df[self.sum_name] - df[self.existing_summands].sum(axis=1)

    def get_description(self) -> str:
        return ""


class SumCompletionRuleGroupCreator:
    """
    completes the missing value of a simple addition, if one is missing.
    E.G Assets = AssetsCurrent + AssetsNoneCurrent
    """

    @staticmethod
    def create_group(sum_tag: str, summand_tags: List[str], prefix: str) -> RuleGroup:
        rules: List[RuleEntity] = [MissingSumRule(sum_name=sum_tag,
                                                  summand_names=summand_tags)]
        for summand in summand_tags:
            others = summand_tags.copy()
            others.remove(summand)
            rules.append(MissingSummandRule(sum_name=sum_tag, missing_summand=summand,
                                            existing_summands=others))
        return RuleGroup(rules=rules, prefix=prefix)


class SumUp(Rule):
    """Sums app the available Summands to a new target column"""

    def __init__(self, target: str, potential_summands: List[str]):
        self.target = target
        self.potential_summands = potential_summands

    def get_target_tags(self) -> List[str]:
        return [self.target]

    def get_input_tags(self) -> Set[str]:
        result = {self.target}
        result.update(self.potential_summands)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        # if the target was not set..
        return df[self.target].isna()

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        df.loc[mask, self.target] = 0.0  # initialize
        for potential_summand in self.potential_summands:
            summand_mask = mask & ~df[potential_summand].isna()
            df.loc[summand_mask, self.target] = df[self.target] + df[potential_summand]

    def get_description(self) -> str:
        return ""


class SetSumIfOneOnlySummand(Rule):
    """
    If the sumt_tag is nan and only one summand has a value, then the
    target gets the copy of the Value and the other summands are set to zero.
    """

    def __init__(self, sum_tag: str, summand_set: str, summands_nan: List[str]):
        self.sum_tag = sum_tag
        self.summand_set = summand_set
        self.summands_nan = summands_nan

    def get_target_tags(self) -> List[str]:
        return [self.sum_tag, *self.summands_nan]

    def get_input_tags(self) -> Set[str]:
        result = {self.sum_tag, self.summand_set}
        result.update(self.summands_nan)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        # if the target was not set..

        mask = df[self.sum_tag].isna() & ~df[self.summand_set].isna()
        for summand_nan in self.summands_nan:
            mask = mask & df[summand_nan].isna()

        return mask

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        df.loc[mask, self.sum_tag] = df[self.summand_set]  # initialize
        for summand_nan in self.summands_nan:
            df.loc[mask, summand_nan] = 0.0

    def get_description(self) -> str:
        return ""

class CleanUpCopyToFirstSummand(Rule):
    """ if the sum_tag is set and the first summand and the other summands are nan,
    then copy the target value to the first summand and set the other summands to '0.0' """

    def __init__(self, sum_tag: str, first_summand: str, other_summands: List[str]):
        self.sum_tag = sum_tag
        self.first_summand = first_summand
        self.other_summands = other_summands

    def get_target_tags(self) -> List[str]:
        return [self.first_summand, *self.other_summands]

    def get_input_tags(self) -> Set[str]:
        result = {self.sum_tag, self.first_summand}
        result.update(self.other_summands)
        return result

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        # if the sum_tag was not set..

        mask = ~df[self.sum_tag].isna() & df[self.first_summand].isna()
        for other_summand in self.other_summands:
            mask = mask & df[other_summand].isna()

        return mask

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        df.loc[mask, self.first_summand] = df[self.sum_tag]  # initialize
        for other_summand in self.other_summands:
            df.loc[mask, other_summand] = 0.0

    def get_description(self) -> str:
        return ""

class CleanUpSumUpCorrections(Rule):
    """ it happens, that the values for Assets and AssetsNoncurrent
    where mixed  up. example: 0001692981-19-000022
    So instead of Assets = AssetsCurrent + AssetsNoncurrent
    it matches AssetsNoncurrent = Assets + AssetsCurrent.

    so the first parameter is the sum_tag, e.g. Assets
    the mixed_up_summand is the name of the summand that was mixed with the sum_tag
    the other_summand is the summand with the correct value, e.g. AssetsCurrent
    """

    def __init__(self, sum_tag: str, mixed_up_summand: str, other_summand: str):
        self.sum_tag = sum_tag
        self.mixed_up_summand = mixed_up_summand
        self.other_summand = other_summand

    def get_target_tags(self) -> List[str]:
        return [self.sum_tag, self.mixed_up_summand]

    def get_input_tags(self) -> Set[str]:
        return {self.sum_tag, self.mixed_up_summand, self.other_summand}

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        return (df[self.mixed_up_summand] == df[self.sum_tag] + df[self.other_summand]) \
               & (df[self.other_summand] > 0)

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        mixed_up_values = df[self.mixed_up_summand].copy()
        df.loc[mask, self.mixed_up_summand] = df[self.sum_tag]
        df.loc[mask, self.sum_tag] = mixed_up_values


    def get_description(self) -> str:
        return ""

class ValidationRule(ABC):

    def __init__(self, id: str):
        self.id = id

    @abstractmethod
    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        pass

    @abstractmethod
    def calculate_error(self, df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        pass

    def validate(self, df: pd.DataFrame):
        mask = self.mask(df)
        error = self.calculate_error(df, mask)

        error_column_name = f'{self.id}_error'
        cat_column_name = f'{self.id}_cat'

        df[error_column_name] = None
        df.loc[mask, error_column_name] = error

        df.loc[mask, cat_column_name] = 100  # gt > 0.1 / 10%
        df.loc[df[error_column_name] <= 0.1, cat_column_name] = 10  # 5-10 %
        df.loc[df[error_column_name] <= 0.05, cat_column_name] = 5  # 1-5 %
        df.loc[df[error_column_name] <= 0.01, cat_column_name] = 1  # < 1%
        df.loc[df[error_column_name] == 0.0, cat_column_name] = 0  # exact match


class SumValidationRule(ValidationRule):

    def __init__(self, id: str, sum_tag: str, summands: List[str]):
        super().__init__(id)
        self.sum_tag = sum_tag
        self.summands = summands

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        mask = ~df[self.sum_tag].isna()
        for summand in self.summands:
            mask = mask & ~df[summand].isna()

        return mask

    def calculate_error(self, df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        return ((df[self.sum_tag] - df[self.summands].sum(axis='columns')) / df[self.sum_tag]).abs()


class RuleProcessor:
    identifier_tags = ['adsh', 'coreg', 'report', 'ddate']

    def __init__(self, rule_tree: RuleGroup, clean_up_rule_tree: RuleGroup,
                 validation_rules: List[ValidationRule],
                 iterations: int, main_tags: List[str], final_tags: List[str],
                 filter_for_main_report: bool = True, invert_negated: bool = True):
        self.rule_tree = rule_tree
        self.clean_up_rule_tree = clean_up_rule_tree
        self.validation_rules = validation_rules
        self.iterations = iterations
        self.main_tags = main_tags
        self.final_tags = final_tags
        self.filter_for_main_report = filter_for_main_report
        self.invert_negated = invert_negated

        self.final_col_order = self.identifier_tags + self.final_tags

        self.preprocess_dupliate_log_df: Optional[pd.DataFrame] = None
        self.log_df: Optional[pd.DataFrame] = None
        self.applied_rules_sum_df: Optional[pd.DataFrame] = None
        self.stats: Optional[pd.DataFrame] = None

    def get_ruletree_descriptions(self):
        # todo
        pass

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        all_input_tags = self.rule_tree.get_input_tags()

        relevant_df = \
            df[['adsh', 'coreg', 'tag', 'version', 'ddate', 'uom', 'value', 'report', 'line',
                'negating']][df.tag.isin(all_input_tags)]

        # preprocessing ..
        #  - deduplicate
        cpy_df = self._deduplicate(relevant_df).copy()

        #  - invert_negated
        if self.invert_negated:
            cpy_df.loc[cpy_df.negating == 1, 'value'] = -cpy_df.value

        # pivot the table, so that the tags become columns
        # todo: folgende Zeile sollte nicht mehr notwendig sein, sobald alle Regeln implementiert sind
        expected_tags = all_input_tags.union(self.final_tags)
        pivot_df = self._pivot(df=cpy_df, expected_tags=expected_tags)

        if self.filter_for_main_report:
            pivot_df = self._filter_pivot_for_main_reports(pivot_df)

        pivot_df_index_cols = ['adsh', 'coreg', 'report', 'ddate', 'uom']
        self.log_df = pivot_df[pivot_df_index_cols].copy()

        total_length = len(pivot_df)

        # calculate_pre_stats:
        pre_apply_df = pivot_df[self.final_col_order]
        self.pre_stats = self._calculate_stats(pivot_df=pre_apply_df)
        self.pre_stats.name = "pre"

        self.stats = pd.DataFrame(self.pre_stats)
        self.stats['pre_rel'] = self.stats.pre / total_length

        self.stats = pd.DataFrame(self.pre_stats)

        for i in range(self.iterations):
            # set ids of the rules in the tree, use the iteration number as prefix
            self.rule_tree.set_id(parent_prefix=f"{i}_")

            # apply the rule_tree
            self.rule_tree.process(df=pivot_df, log_df=self.log_df)

            self.post_stats = self._calculate_stats(pivot_df=pivot_df)
            self.post_stats.name = f"post_{i}"

            self.stats = self.stats.join(self.post_stats)
            self.stats[self.post_stats.name + '_rel'] = \
                self.stats[self.post_stats.name] / total_length
            self.stats[self.post_stats.name + '_red'] = \
                1 - (self.stats[self.post_stats.name] / self.stats.pre)

        # clean up rules
        self.clean_up_rule_tree.process(df=pivot_df, log_df=self.log_df)

        # calculate cleanup stats
        self.post_cleanup = self._calculate_stats(pivot_df=pivot_df)
        self.post_cleanup.name = "cleanup"
        self.stats = self.stats.join(self.post_cleanup)
        self.stats[self.post_cleanup.name + '_rel'] = \
            self.stats[self.post_cleanup.name] / total_length
        self.stats[self.post_cleanup.name + '_red'] = \
            1 - (self.stats[self.post_cleanup.name] / self.stats.pre)

        # create a meaningful order
        pivot_df = pivot_df[self.final_col_order].copy()

        # apply validation rules
        for  validation_rule in self.validation_rules:
            validation_rule.validate(pivot_df)

        # caculate log_df summaries
        if self.log_df is not None:
            # filter for rule columns but making sure the order stays the same
            rule_columns = [x for x in self.log_df.columns if x not in pivot_df_index_cols]
            self.applied_rules_sum_df = self.log_df[rule_columns].sum()


        return pivot_df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        # find duplicated entries
        # sometimes, only single tags are duplicated, however, there are also reports
        # where all tags of a report are duplicated, maybe due to a problem with processing
        duplicates_s = \
            df.duplicated(['adsh', 'coreg', 'report', 'uom', 'tag', 'version', 'ddate', 'value'])

        self.preprocess_dupliate_log_df = df[duplicates_s] \
            [['adsh', 'coreg', 'report', 'tag', 'uom', 'version', 'ddate']].copy()

        return df[~duplicates_s]

    def _filter_pivot_for_main_reports(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        """ Some reports have more than one 'report number' (column report) for a
            certain statement. Generally, the one with the most tags is the one to take.
            This method operates on the pivoted data and counts the none-values of the
            "main columns". The main columns are the fields, that generally should be there.
             """

        cpy_pivot_df = pivot_df.copy()

        cpy_pivot_df['nan_count'] = cpy_pivot_df[self.main_tags].isna().sum(axis=1)
        cpy_pivot_df.sort_values(['adsh', 'coreg', 'nan_count'], inplace=True)
        cpy_pivot_df = cpy_pivot_df.groupby(['adsh', 'coreg']).last()
        cpy_pivot_df.reset_index(inplace=True)
        return cpy_pivot_df

    def _calculate_stats(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        return pivot_df[self.final_tags].isna().sum(axis=0)

    def _pivot(self, df: pd.DataFrame, expected_tags: Set[str]) -> pd.DataFrame:
        # it is possible, that the same number appears multiple times in different lines,
        # therefore, duplicates have to be removed, otherwise pivot is not possible
        relevant_df = df[
            ['adsh', 'coreg', 'report', 'tag', 'value', 'uom', 'ddate']].drop_duplicates()

        pivot_df = relevant_df.pivot(index=['adsh', 'coreg', 'report', 'uom', 'ddate'],
                                     columns='tag',
                                     values='value')
        pivot_df.reset_index(inplace=True)
        missing_cols = set(expected_tags) - set(pivot_df.columns)
        for missing_col in missing_cols:
            pivot_df[missing_col] = np.nan
        pivot_df['nan_count'] = np.nan
        return pivot_df


class BalanceSheetStandardizer(RuleProcessor):
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
            CopyTagRule(original='LiabilitiesAndStockholdersEquity', target='LiabilitiesAndOwnerEquity')

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
        rules=[SumUp(
            target='CashOther',
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
            SumCompletionRuleGroupCreator.create_group(
                sum_tag='Assets',
                summand_tags=['AssetsCurrent', 'AssetsNoncurrent'],
                prefix="Ass"
            ),
            SetSumIfOneOnlySummand(
                sum_tag='Assets',
                summand_set='AssetsCurrent',
                summands_nan=['AssetsNoncurrent']
            ),
            SetSumIfOneOnlySummand(
                sum_tag='Assets',
                summand_set='AssetsNoncurrent',
                summands_nan=['AssetsCurrent']
            ),
            SumCompletionRuleGroupCreator.create_group(
                sum_tag='Liabilities',
                summand_tags=['LiabilitiesCurrent', 'LiabilitiesNoncurrent'],
                prefix="Lia"
            ),
            SetSumIfOneOnlySummand(
                sum_tag='Liabilities',
                summand_set='LiabilitiesCurrent',
                summands_nan=['LiabilitiesNoncurrent']
            ),
            SetSumIfOneOnlySummand(
                sum_tag='Liabilities',
                summand_set='LiabilitiesNoncurrent',
                summands_nan=['LiabilitiesCurrent']
            ),
            SumCompletionRuleGroupCreator.create_group(
                sum_tag='AssLiaOwe',
                summand_tags=['Liabilities', 'OwnerEquity'],
                prefix="Ass2"
            ),
            SumCompletionRuleGroupCreator.create_group(
                sum_tag='LiabilitiesAndOwnerEquity',
                summand_tags=['Liabilities', 'OwnerEquity'],
                prefix="LiaOwnEq"
            )
        ])

    rule_tree = RuleGroup(prefix="BS_",
                          rules=[
                              bs_rename_rg,
                              bs_sumup_rg,
                              bs_sum_completion,
                          ])

    cleanup_rule_tree = RuleGroup(prefix="BS_CL_",
                                  rules=[
                                      CleanUpCopyToFirstSummand(sum_tag='Assets',
                                                                first_summand='AssetsCurrent',
                                                                other_summands=[
                                                                    'AssetsNoncurrent']),
                                      CleanUpCopyToFirstSummand(sum_tag='Liabilities',
                                                                first_summand='LiabilitiesCurrent',
                                                                other_summands=[
                                                                    'LiabilitiesNoncurrent']),
                                      CleanUpSumUpCorrections(sum_tag='Assets',
                                                              mixed_up_summand='AssetsNoncurrent',
                                                              other_summand='AssetsCurrent'),
                                      CleanUpSumUpCorrections(sum_tag='Assets',
                                                              mixed_up_summand='AssetsCurrent',
                                                              other_summand='AssetsNoncurrent'),
                                  ])

    validation_rules: List[ValidationRule] = [
        SumValidationRule(id='AssetsCheck',
                          sum_tag='Assets',
                          summands=['AssetsCurrent', 'AssetsNoncurrent']),
        SumValidationRule(id='LiabilitiesCheck',
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
    main_tags = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                 'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent']

    def __init__(self, filter_for_main_report: bool = False, iterations: int = 3):
        super().__init__(rule_tree=self.rule_tree,
                         clean_up_rule_tree=self.cleanup_rule_tree,
                         validation_rules=self.validation_rules,
                         iterations=iterations,
                         main_tags=self.main_tags, final_tags=self.final_tags,
                         filter_for_main_report=filter_for_main_report)


# there are pre-pivot and post-pivot rules ..
# in order to make a pre assesement on how many fields could be filled, we would need to have
# pivot run without applying any rules, just pivot and return the desired colums,
# or just the basic renaming ...
# but without going for changing default names

# man könnte noch checks einbauen, ob z.B. die Summen definition gültig sind,
# d.h., dass der Wert der Summe in etwa der Summe der Werte der Summanden entsprciht


class BalanceSheetStandardizerOld:

    def fill_still_isna_summands(self, df: pd.DataFrame):
        """ if we are left with unset summands"""

        # for:
        # 'Assets': ('AssetsCurrent', 'AssetsNoncurrent'),
        # 'Liabilities': ('LiabilitiesCurrent', 'LiabilitiesNoncurrent')
        for total, summands in BalanceSheetStandardizerOld.sum_definition.items():
            summand_1 = summands[0]
            summand_2 = summands[1]

            # if only the total is present, we assume that only the first summand (eg. currentassets are present)
            # that is not really proper.
            mask_total_nosum1_nosum2 = (~df[total].isna() &
                                        df[summand_1].isna() &
                                        df[summand_2].isna())
            df.loc[mask_total_nosum1_nosum2, summand_1] = df[total]
            df.loc[mask_total_nosum1_nosum2, summand_2] = 0.0

            # if only the first summand is present, wie assume that the total is equal to the first summand
            # and that the second summand is 0
            mask_nototal_sum1_nosum2 = (df[total].isna() &
                                        ~df[summand_1].isna() &
                                        df[summand_2].isna())
            df.loc[mask_nototal_sum1_nosum2, total] = df[summand_1]
            df.loc[mask_nototal_sum1_nosum2, summand_2] = 0.0

            # if only the second summand is present, wie assume that the total is equal to the second summand
            # and that the first  summand is 0
            mask_nototal_nosum1_sum2 = (df[total].isna() &
                                        df[summand_1].isna() &
                                        ~df[summand_2].isna())
            df.loc[mask_nototal_nosum1_sum2, total] = df[summand_2]
            df.loc[mask_nototal_nosum1_sum2, summand_1] = 0.0

    def complete_special_rules(self, df: pd.DataFrame):
        """
        Special rules which help to complete the information
        """

        # self._basic_sum_completion(df, total='Assets', summand_1='Liabilities',
        #                            summand_2='Equity')
        #
        # # actually, LiabilitiesAndStockholdersEquity should have the same value as Assets
        # self._basic_sum_completion(df, total='LiabilitiesAndStockholdersEquity',
        #                            summand_1='Liabilities',
        #                            summand_2='Equity')

        # run complete sum again, since total fields could be present now
        # for:
        # 'Assets': ('AssetsCurrent', 'AssetsNoncurrent'),
        # 'Liabilities': ('LiabilitiesCurrent', 'LiabilitiesNoncurrent')
        self.complete_sums(df)

        self.fill_still_isna_summands(df)

        # if Equity == -Liabilities and Assets is nan, then
        # Assets, AssetsCurrent, and AssetsNoncurrent is 0.0
        mask = (df.Equity == -df.Liabilities) & df.Assets.isna()
        df.loc[mask, 'Assets'] = 0.0
        df.loc[mask, 'AssetsCurrent'] = 0.0
        df.loc[mask, 'AssetsNoncurrent'] = 0.0

        # if Cash is not set, then use CashOther to set Cash
        mask = df.Cash.isna()
        df.loc[mask, 'Cash'] = df.CashOther

        # set to zero
        mask = df.InventoryNet.isna()
        df.loc[mask, 'InventoryNet'] = 0.0
