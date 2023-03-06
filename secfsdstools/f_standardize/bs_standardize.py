from typing import Dict, List, Tuple

import pandas as pd


class BalanceSheetStandardizer:
    rename_map: Dict[str, str] = {
        'AssetsNet': 'Assets',
        'PartnersCapital': 'Equity',
        'StockholdersEquity': 'Equity',
        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest': 'Equity'
    }
    relevant_tags: List[str] = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                                'InventoryNet', 'CashAndCashEquivalentsAtCarryingValue',
                                'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
                                'RetainedEarningsAccumulatedDeficit',
                                'Equity',
                                'LiabilitiesAndStockholdersEquity'
                                ]

    sum_definition: Dict[str, Tuple[str, str]] = {
        'Assets': ('AssetsCurrent', 'AssetsNoncurrent'),
        'Liabilities': ('LiabilitiesCurrent', 'LiabilitiesNoncurrent')
    }

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        # todo: wann muss negating berücksichtigt werden?

        cpy_df = df[(df.stmt == 'BS')] \
            [['adsh', 'coreg', 'tag', 'version', 'ddate', 'uom', 'value', 'report', 'line', 'negating']].copy()

        self.rename_tags(cpy_df)
        cpy_df = cpy_df[cpy_df.tag.isin(BalanceSheetStandardizer.relevant_tags)]
        pivot_df = self.pivot(cpy_df)
        self.complete_sums(pivot_df)
        return pivot_df

    def rename_tags(self, df: pd.DataFrame):
        """ renames AssetsNet to Assets"""
        for old_tag, new_tag in BalanceSheetStandardizer.rename_map.items():
            mask = (df.tag == old_tag)
            df.loc[mask, 'tag'] = new_tag

    def pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        # it is possible, that the same number appears multiple times in different lines,
        # therefore, duplicates have to be removed, otherwise pivot is not possible
        relevant_df = df[['adsh', 'coreg', 'report', 'tag', 'value']].drop_duplicates()

        relevant_df.loc[relevant_df['coreg'].isna(), 'coreg'] = '-'

        duplicates_df = relevant_df[['adsh', 'coreg', 'report', 'tag']].value_counts().reset_index()
        duplicates_df.rename(columns={0: 'count'}, inplace=True)
        duplicated_adsh = duplicates_df[duplicates_df['count'] > 1].adsh.unique().tolist()

        # todo: logging duplicated entries ...>
        relevant_df = relevant_df[~relevant_df.adsh.isin(duplicated_adsh)]

        # for adsh in relevant_df.adsh.tolist():
        #     print(adsh)
        #     relevant_df[relevant_df.adsh==adsh].pivot(index=['adsh', 'coreg', 'report'],
        #                                                             columns='tag',
        #                                                             values='value')

        pivot_df = relevant_df.pivot(index=['adsh', 'coreg', 'report'],
                                     columns='tag',
                                     values='value')
        pivot_df.reset_index(inplace=True)
        return pivot_df

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

            # if only the total is present, we assume that only the first summand (eg. currentassets are present)
            # that is not really proper.
            mask_total_nosum1_nosum2 = (~df[total].isna() &
                                        df[summand_1].isna() &
                                        df[summand_2].isna())
            df.loc[mask_total_nosum1_nosum2, summand_1] = df[total]
            df.loc[mask_total_nosum1_nosum2, summand_2] = 0.0

            mask_nototal_sum1_nosum2 = (df[total].isna() &
                                        ~df[summand_1].isna() &
                                        df[summand_2].isna())
            df.loc[mask_nototal_sum1_nosum2, total] = df[summand_1]
            df.loc[mask_nototal_sum1_nosum2, summand_2] = 0.0

            mask_nototal_nosum1_sum2 = (df[total].isna() &
                                        df[summand_1].isna() &
                                        ~df[summand_2].isna())
            df.loc[mask_nototal_nosum1_sum2, total] = df[summand_2]
            df.loc[mask_nototal_nosum1_sum2, summand_1] = 0.0


            oft ist auch nur cash vorhanden -> vorallem bei subsidiaries -> hier müsste man prüfen, ob
            in dem fall cash == assets ist
            manchmal ist diese info auch in einem zusätzlichen report/tabelle
            -> dort könnte man das eigentlich ignorieren
            -> man müsste also prüfen, ob für die gleiche kombination ovn
            -> adsh und coreg bereits ein eintrag besteht.

            es bräuchte so etwas wie zusätzliche clean regeln,
            z.B. nur einträge mit weniger nan beachten, falls nur report unterschiedlich
            oder generell nur tiefere report nr beachten.

            man könnte auch noch coregs entfernen und nur main zulassen..

            das ziel müsste sein, die main einträge möglichst vollständig zu haben.

