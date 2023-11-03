from typing import List

import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.joinedfiltering import USDonlyFilter as USDonlyJoined
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregFilter, USDonlyFilter
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer

apple_10k_2017 = "0000320193-17-000070"


def get_fs_from_zip() -> pd.DataFrame:
    reader = ZipCollector(datapath="../data/parquet/quarter/2017q4.zip", stmt_filter=['BS'],
                          forms_filter=['10-K', '10-Q'])
    return reader.collect()[MainCoregFilter()][ReportPeriodRawFilter()].join().get_pre_num_copy()


def get_fs_from_all_bs() -> pd.DataFrame:
    save_path_BS_all = "./saved_data/bs_10k_10q_all_joined"
    return JoinedDataBag.load(save_path_BS_all)[USDonlyJoined()].get_pre_num_copy()


def get_fs_from_single_report(adsh: str) -> pd.DataFrame:
    reader = SingleReportCollector.get_report_by_adsh(adsh=adsh, stmt_filter=['BS'])
    return reader.collect()[MainCoregFilter()][ReportPeriodRawFilter()][
        USDonlyFilter()].join().get_pre_num_copy()


def get_uniques_col(df: pd.DataFrame, col: str) -> List[str]:
    return df[col].unique().tolist()


def get_count_tags_per_adsh(df: pd.DataFrame, tags: List[str]) -> pd.DataFrame:

    return df[['adsh', 'report', 'tag']][df.tag.isin(tags)].groupby(['adsh', 'report']).count().reset_index()


def check_uom_list(df: pd.DataFrame):
    uom_list = get_uniques_col(fs_df, "uom")
    print(uom_list)


def check_for_equity_tags(df: pd.DataFrame):
    counts_df = get_count_tags_per_adsh(df, ['StockholdersEquity', 'PartnerCapital'])
    result = counts_df[counts_df.tag > 1]
    print(result.shape)
    # filter where more than 1 tag with stockholdersquity and partnercapital are present
    filterd = df[df.adsh.isin(result.adsh.tolist()) & df.tag.isin(['StockholdersEquity', 'PartnerCapital'])]
    # first checks shows, that only stockholderequity appears in reports where
    # appears more than once
    # -> hence StockholdersEquity and PartnerCapital never appear together
    # however, we have reports which have the entries doubled -> all bs lines appear twice.
    # with the same value
    print(filterd.tag.unique().tolist())



def check_drop_out_mask(df: pd.DataFrame):
    """
    groups by adsh and report and counts the instances of Assets and StockholdersEquity
    per group, which is a Series.
    Transform that Series into a Dataframe with columns for adsh and tag
    """

    duplicates = df.duplicated(['adsh', 'coreg', 'report', 'uom', 'tag', 'version', 'ddate', 'value'])

    counts_s = df[['adsh', 'report', 'tag']][df.tag.isin(['Assets', 'StockholdersEquity'])].groupby(['adsh', 'report'])['tag'].value_counts()

    new_df = pd.DataFrame(counts_s.to_numpy(), columns=['tag_count'])

    for level in counts_s.index.levels:
        new_df[level.name] = counts_s.index.get_level_values(level.name)

    return new_df


if __name__ == '__main__':
    #fs_df = get_fs_from_single_report('0001171520-13-000365')

    fs_df = get_fs_from_all_bs()
    # check_drop_out_mask(fs_df)
    # check_for_equity_tags(fs_df)
    # fs_df = fs_df[fs_df.adsh.isin(['0001096906-17-000798'])]

    # fs_df_retained_count = fs_df[['tag', 'adsh']][
    #     fs_df.tag.str.startswith('RetainedEarnings')].groupby('adsh').count()
    # fs_df_retained_count_gt_1 = fs_df_retained_count[fs_df_retained_count.tag > 1].reset_index()

    print("fs_df.shape", fs_df.shape)

    standardizer = BalanceSheetStandardizer(filter_for_main_report=True)
    df = standardizer.process(fs_df)

    #  hier wäare so eine art statistik gut, we sieht die sitatuion vor dem bereinigen aus,
    # also wieviele enträge fehlen, und wie sieht es danach aus..
    # die operationen sollten auch noch Risiko profil haben, also z.B.
    # den dritten Wert auszurechnen, wenn zwei Werte vorhanden sind ist ein tiefes risiko
    # oder wenn es einfach andere bezeichnungen für dasselbe gibt, ebenfalls.
    # aber wenn es dann ein wenig komplizierter ist,

    df_missing_assets = df[df.Assets.isna() | df.AssetsCurrent.isna() | df.AssetsNoncurrent.isna()]

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
