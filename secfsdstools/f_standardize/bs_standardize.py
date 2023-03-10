from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


class BalanceSheetStandardizer:
    rename_map: Dict[str, str] = {
        'AssetsNet': 'Assets',
        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest': 'Equity',
        'PartnersCapital': 'Equity',
        'StockholdersEquity': 'Equity',
        'CashAndCashEquivalentsAtCarryingValue': 'Cash',
        'RetainedEarningsAppropriated': 'RetainedEarnings',
        'RetainedEarningsAccumulatedDeficit': 'RetainedEarnings'
    }

    # several tag values are sumed upd into a new tag
    sum_list_map: Dict[str, List[str]] = {'CashOther': [
        'CashAndCashEquivalentsAtFairValue',
        'CashAndDueFromBanks',
        'CashCashEquivalentsAndFederalFundsSold',
        'RestrictedCashAndCashEquivalentsAtCarryingValue',
        'CashAndCashEquivalentsInForeignCurrencyAtCarryingValue']}

    # these are the relevant tags that we need to calculate missing values
    relevant_tags: List[str] = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                                'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
                                'Equity',
                                'InventoryNet',
                                'Cash',
                                'CashOther',
                                'RetainedEarnings',
                                'LiabilitiesAndStockholdersEquity'
                                ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                             'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
                             'Equity',
                             'InventoryNet',
                             'Cash',
                             'RetainedEarnings'
                             ]

    # used to evaluate if a report is a the main balancesheet report
    # inside a report, there can be several different tables (different report nr)
    # which stmt value is BS.
    # however, we might be only interested in the "major" BS report. Usually this is the
    # one which has the least nan in the following columns
    main_columns = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                    'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent']

    # sum definition, which is used to calculate missing values, if eithter the total
    # or one of the summand is missing
    sum_definition: Dict[str, Tuple[str, str]] = {
        'Assets': ('AssetsCurrent', 'AssetsNoncurrent'),
        'Liabilities': ('LiabilitiesCurrent', 'LiabilitiesNoncurrent')
    }

    def standardize(self, df: pd.DataFrame, only_return_main_coreg: bool = False,
                    filter_for_main_report: bool = False) -> pd.DataFrame:

        # todo: wann muss negating berücksichtigt werden?
        cpy_df = df[(df.stmt == 'BS')] \
            [['adsh', 'coreg', 'tag', 'version', 'ddate', 'uom', 'value', 'report', 'line', 'negating']].copy()

        cpy_df.loc[cpy_df['coreg'].isna(), 'coreg'] = '-'

        # rename tags
        self.rename_tags(cpy_df)

        # sum tags up into a new sum-tag
        cpy_df = self.handle_sum_map(cpy_df)

        # filter the columns, only us the columns we need for the next steps
        cpy_df = cpy_df[cpy_df.tag.isin(BalanceSheetStandardizer.relevant_tags)]

        # pivot the table, so that the tags are now columns
        pivot_df = self.pivot(cpy_df)

        # complete the total = sum1 + sum2 tripples based on the sum_definition
        self.complete_sums(pivot_df)

        # apply the final rules
        self.complete_special_rules(pivot_df)

        # check if only the main coreg shall be returned
        if only_return_main_coreg:
            pivot_df = pivot_df[pivot_df.coreg == '-']

        # check if only the main BS report shall be returned
        if filter_for_main_report:
            pivot_df['nan_count'] = pivot_df[BalanceSheetStandardizer.main_columns].isna().sum(axis=1)
            pivot_df.sort_values(['adsh', 'coreg', 'nan_count'], inplace=True)
            pivot_df = pivot_df.groupby(['adsh', 'coreg']).last()
            pivot_df.reset_index(inplace=True)

        # create a meaningful order
        col_order = ['adsh', 'coreg', 'report', 'ddate'] + BalanceSheetStandardizer.final_tags
        return pivot_df[col_order].copy()

    def rename_tags(self, df: pd.DataFrame):
        """ renames AssetsNet to Assets"""
        for old_tag, new_tag in BalanceSheetStandardizer.rename_map.items():
            mask = (df.tag == old_tag)
            df.loc[mask, 'tag'] = new_tag

    def handle_sum_map(self, df: pd.DataFrame) -> pd.DataFrame:
        append_list: List[pd.DataFrame] = []
        append_list.append(df)
        for tag, sum_list in BalanceSheetStandardizer.sum_list_map.items():
            result_df = df[df.tag.isin(sum_list)][['adsh', 'coreg', 'report', 'ddate', 'value']] \
                .groupby(['adsh', 'coreg', 'report', 'ddate']).sum().reset_index()
            result_df['tag'] = tag
            result_df['line'] = 0
            result_df['negating'] = 0
            result_df['uom'] = ''
            result_df['version'] = ''
            append_list.append(result_df)

        return pd.concat(append_list).reset_index()

    def pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        # it is possible, that the same number appears multiple times in different lines,
        # therefore, duplicates have to be removed, otherwise pivot is not possible
        relevant_df = df[['adsh', 'coreg', 'report', 'tag', 'value', 'ddate']].drop_duplicates()

        duplicates_df = relevant_df[['adsh', 'coreg', 'report', 'tag', 'ddate']].value_counts().reset_index()
        duplicates_df.rename(columns={0: 'count'}, inplace=True)
        duplicated_adsh = duplicates_df[duplicates_df['count'] > 1].adsh.unique().tolist()

        # todo: logging duplicated entries ...>
        relevant_df = relevant_df[~relevant_df.adsh.isin(duplicated_adsh)]

        pivot_df = relevant_df.pivot(index=['adsh', 'coreg', 'report', 'ddate'],
                                     columns='tag',
                                     values='value')
        pivot_df.reset_index(inplace=True)
        missing_cols = set(BalanceSheetStandardizer.relevant_tags) - set(pivot_df.columns)
        for missing_col in missing_cols:
            pivot_df[missing_col] = np.nan

        return pivot_df

    def _basic_sum_completion(self, df: pd.DataFrame, total: str, summand_1: str, summand_2: str):
        """
            calculates missing summaries total = summand_1 + summand_2, if one of them is missing
        """

        mask_total_sum1_nosum2 = (~df[total].isna() &
                                  ~df[summand_1].isna() &
                                  df[summand_2].isna())
        df.loc[mask_total_sum1_nosum2, summand_2] = df[total] - df[summand_1]

        mask_total_nosum1_sum2 = (~df[total].isna() &
                                  df[summand_1].isna() &
                                  ~df[summand_2].isna())
        df.loc[mask_total_nosum1_sum2, summand_1] = df[total] - df[summand_2]

        mask_nototal_sum1_sum2 = (df[total].isna() &
                                  ~df[summand_1].isna() &
                                  ~df[summand_2].isna())
        df.loc[mask_nototal_sum1_sum2, total] = df[summand_1] + df[summand_2]

    def complete_sums(self, df: pd.DataFrame):
        """ completes sum definitions:
            eg. Assets = 'AssetsCurrent' + 'AssetsNoncurrent'
            if Assets and AssetsCurrent is present -> calculate AssetsNoncurrent
            if Assets and AssetsNoncurrent is present -> calculate AssetsCurrent
            if AssetsNoncurrent and AssetsCurrent are present, calculate Assets
        """
        for total, summands in BalanceSheetStandardizer.sum_definition.items():
            summand_1 = summands[0]
            summand_2 = summands[1]

            self._basic_sum_completion(df, total=total, summand_1=summand_1, summand_2=summand_2)

    def fill_still_isna_summands(self, df: pd.DataFrame):
        """ if we are left with unset summands"""

        for total, summands in BalanceSheetStandardizer.sum_definition.items():
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

        self._basic_sum_completion(df, total='Assets', summand_1='Liabilities', summand_2='Equity')

        # actually, LiabilitiesAndStockholdersEquity should have the same value as Assets
        self._basic_sum_completion(df, total='LiabilitiesAndStockholdersEquity', summand_1='Liabilities',
                                   summand_2='Equity')

        # run complete sum again, since total fields could be present now
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
