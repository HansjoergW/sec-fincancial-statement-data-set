import pandas as pd


class BalanceSheetStandardizer:

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        cpy_df = df.copy()
        self.handle_assetsnet(cpy_df)
        return cpy_df

    def handle_assetsnet(self, df: pd.DataFrame):
        """ renames AssetsNet to Assets"""
        mask = (df.tag == 'AssetsNet') & (df.stmt == 'BS')
        df.loc[mask, 'tag'] = 'Assets'

    def handle_assetscur_assetsnoncur_present(self, df: pd.DataFrame):
        """calculateed Assets if only assets current and noncurrent are present"""

        mask = (df.tag == 'AssetsNet') & (df.stmt == 'BS')


Frage: wann piovtieren wir? sonst müssen wir viele groupbys und rows add machen
und auf welches Datum?

das müsste man von anfang her anders selektieren, period dürfte nicht pivotiert
werden

man müsste ein raw df haben, dass nur period dates enthält, aber nicht pivotiert ist

das wäre der nächste Schritt.
hier müsste ich auch überlegen, welche spalten ich benötige
z.B.sollte negating in die Rechnung einbezogen werden.

der balance sheet standardizer erzeugt dann eine BS only übersicht


steps gemäss dipl arbeit.
- erst renaming


