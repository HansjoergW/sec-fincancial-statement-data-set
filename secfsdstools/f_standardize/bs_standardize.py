from typing import Dict, List

import pandas as pd


class BalanceSheetStandardizer:
    rename_map: Dict[str, str] = {
        'AssetsNet': 'Assets',
        'PartnersCapital': 'Equity',
        'StockholdersEquity': 'Equity',
        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest': 'Equity'
    }
    relevant_tags: List[str] = ['Assets', 'AssetsCurrent', 'AssetsNoncurrent',
                                'InventoryNet', 'CashAndCashEquivalentsAtCarrying',
                                'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
                                'RetainedEarningsAccumulatedDeficit'
                                'Equity'
                                ]

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        # todo: wann muss negating berücksichtigt werden?

        cpy_df = df[(df.stmt == 'BS') & df.tag.isin(BalanceSheetStandardizer.relevant_tags)] \
            [['adsh', 'coreg', 'tag', 'version', 'ddate', 'uom', 'value', 'report', 'line', 'negating']].copy()

        self.rename_tags(cpy_df)
        pivot_df = self.pivot(cpy_df)
        return cpy_df

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

        Equity Tag ist nicht vorhanden

        return relevant_df.pivot(index=['adsh', 'coreg', 'report'],
                                 columns='tag',
                                 values='value')

    def handle_assetscur_assetsnoncur_present(self, df: pd.DataFrame):
        """calculateed Assets if only assets current and noncurrent are present"""


"""



steps gemäss dipl arbeit.
- erst renaming

"""
