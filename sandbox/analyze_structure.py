import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregFilter
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer

apple_10k_2017 = "0000320193-17-000070"


def get_fs_from_zip() -> pd.DataFrame:
    reader = ZipCollector(datapath="../data/parquet/quarter/2017q4.zip", stmt_filter=['BS'],
                          forms_filter=['10-K', '10-Q'])
    return reader.collect()[MainCoregFilter()][ReportPeriodRawFilter()].join().get_pre_num_copy()


def get_fs_from_all_bs() -> pd.DataFrame:
    save_path_BS_all = "./saved_data/bs_10k_10q_all_joined"
    return JoinedDataBag.load(save_path_BS_all).get_pre_num_copy()


def get_fs_from_single_report(adsh: str) -> pd.DataFrame:
    reader = SingleReportCollector.get_report_by_adsh(adsh=adsh, stmt_filter=['BS'])
    return reader.collect()[MainCoregFilter()][ReportPeriodRawFilter()].join().get_pre_num_copy()


if __name__ == '__main__':
    fs_df = get_fs_from_single_report('0001828937-22-000020') # Non Current and NonCurrent missing

    # fs_df = get_fs_from_all_bs()
    # fs_df = fs_df[fs_df.adsh.isin(['0001096906-17-000798'])]

    print("fs_df.shape", fs_df.shape)

    standardizer = BalanceSheetStandardizer(filter_for_main_report=True)
    df = standardizer.process(fs_df)

    #  hier wäare so eine art statistik gut, we sieht die sitatuion vor dem bereinigen aus,
    # also wieviele enträge fehlen, und wie sieht es danach aus..
    # die operationen sollten auch noch Risiko profil haben, also z.B.
    # den dritten Wert auszurechnen, wenn zwei Werte vorhanden sind ist ein tiefes risiko
    # oder wenn es einfach andere bezeichnungen für dasselbe gibt, ebenfalls.
    # aber wenn es dann ein wenig komplizierter ist,

    adshs_set = set(df.adsh.unique().tolist())
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
