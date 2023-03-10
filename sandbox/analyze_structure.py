from secfsdstools.e_read.zipreportreading import ZipReportReader
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer




if __name__ == '__main__':

    apple_10k_2017 = "0000320193-17-000070"
    reader = ZipReportReader("../data/dld/2017q4.zip")

    fs_df = reader.merge_for_period()
    fs_df = fs_df[fs_df.form.isin(['10-K', '10-Q'])]
    #fs_df = fs_df[fs_df.adsh.isin(['0001096906-17-000798'])]

    standardizer = BalanceSheetStandardizer()

    df = standardizer.standardize(fs_df, only_return_main_coreg=True, filter_for_main_report=True)

    adshs_set =  set(df.adsh.unique().tolist())
    adshs_with_assets = set(df[df.tag.isin(['Assets'])].adsh.unique().tolist())

    missing_assets = adshs_set - adshs_with_assets

    fs_missing_df = df[df.adsh.isin(missing_assets) & df.stmt.isin(['BS'])].copy()

    print(fs_missing_df.groupby('adsh').size().reset_index(name='count'))


    print(len(missing_assets))
    print(len(fs_df.adsh.unique()))
    print(fs_df.columns)




