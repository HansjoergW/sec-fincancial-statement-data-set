from secfsdstools.e_read.zipreportreading import ZipReportReader





if __name__ == '__main__':

    apple_10k_2017 = "0000320193-17-000070"
    reader = ZipReportReader("../data/dld/2017q4.zip")

    fs_df = reader.financial_statements_for_period()

    adshs_set =  set(fs_df.adsh.unique().tolist())
    adshs_with_assets = set(fs_df[fs_df.tag.isin(['Assets', 'AssetsNet'])].adsh.unique().tolist())

    missing_assets = adshs_set - adshs_with_assets

    fs_missing_df = fs_df[fs_df.adsh.isin(missing_assets) & fs_df.stmt.isin(['BS'])].copy()

    fs_missing_df.groupby('adsh').size().reset_index(name='count')

    print(len(missing_assets))
    print(len(fs_df.adsh.unique()))
    print(fs_df.columns)




