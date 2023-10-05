from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregFilter
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizerOld, BalanceSheetStandardizer

import pandas as pd


if __name__ == '__main__':

    apple_10k_2017 = "0000320193-17-000070"
    reader = ZipCollector(datapath="../data/parquet/quarter/2017q4.zip", stmt_filter=['BS'], forms_filter=['10-K', '10-Q'])
    fs_df = reader.collect()[MainCoregFilter()][ReportPeriodRawFilter()].join().get_pre_num_copy()

    #fs_df = fs_df[fs_df.adsh.isin(['0001096906-17-000798'])]

    # standardizer = BalanceSheetStandardizerOld()
    # df = standardizer.standardize(fs_df, only_return_main_coreg=True, filter_for_main_report=True)
    standardizer = BalanceSheetStandardizer(filter_for_main_report=True)
    df = standardizer.standardize(fs_df)

    #  hier wäare so eine art statistik gut, we sieht die sitatuion vor dem bereinigen aus,
    # also wieviele enträge fehlen, und wie sieht es danach aus..
    # die operationen sollten auch noch Risiko profil haben, also z.B.
    # den dritten Wert auszurechnen, wenn zwei Werte vorhanden sind ist ein tiefes risiko
    # oder wenn es einfach andere bezeichnungen für dasselbe gibt, ebenfalls.
    # aber wenn es dann ein wenig komplizierter ist,



    adshs_set =  set(df.adsh.unique().tolist())
    adshs_with_assets = set(df[~df.Assets.isnull()].adsh.unique().tolist())

    missing_assets = adshs_set - adshs_with_assets

    fs_missing_df = df[df.adsh.isin(missing_assets)].copy()

    print(fs_missing_df.groupby('adsh').size().reset_index(name='count'))


    print(len(missing_assets))
    print(len(fs_df.adsh.unique()))
    print(fs_df.columns)

    # standardizer.pre_stats.name="pre"
    # pre_stats_df = pd.DataFrame(standardizer.pre_stats)
    # standardizer.post_stats.name="post"
    # joined_df = pre_stats_df.join(standardizer.post_stats)
    # joined_df['reduction'] = 1 - (joined_df.post / joined_df.pre)
    # print(joined_df)




