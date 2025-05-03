from typing import List

import pandas as pd
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.joinedfiltering import USDOnlyJoinedFilter
from secfsdstools.e_filter.rawfiltering import MainCoregRawFilter, ReportPeriodRawFilter, USDOnlyRawFilter
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer

apple_10k_2017 = "0000320193-17-000070"


def get_fs_from_zip() -> pd.DataFrame:
    reader = ZipCollector(datapath="../data/parquet/quarter/2017q4.zip", stmt_filter=['BS'],
                          forms_filter=['10-K', '10-Q'])
    return reader.collect()[MainCoregRawFilter()][ReportPeriodRawFilter()].join().get_pre_num_copy()


def get_fs_from_all_bs() -> pd.DataFrame:
    save_path_BS_all = "./saved_data/bs_10k_10q_all_joined"
    return JoinedDataBag.load(save_path_BS_all)[USDOnlyJoinedFilter()].get_pre_num_copy()


def get_fs_from_single_report(adsh: str) -> pd.DataFrame:
    reader = SingleReportCollector.get_report_by_adsh(adsh=adsh, stmt_filter=['BS'])
    return reader.collect()[MainCoregRawFilter()][ReportPeriodRawFilter()][
        USDOnlyRawFilter()].join().get_pre_num_copy()


def get_uniques_col(df: pd.DataFrame, col: str) -> List[str]:
    return df[col].unique().tolist()


def get_count_tags_per_adsh(df: pd.DataFrame, tags: List[str]) -> pd.DataFrame:
    return df[['adsh', 'report', 'tag']][df.tag.isin(tags)].groupby(
        ['adsh', 'report']).count().reset_index()


def check_uom_list(df: pd.DataFrame):
    uom_list = get_uniques_col(fs_df, "uom")
    print(uom_list)


def check_for_equity_tags(df: pd.DataFrame):
    counts_df = get_count_tags_per_adsh(df, ['StockholdersEquity', 'PartnerCapital'])
    result = counts_df[counts_df.tag > 1]
    print(result.shape)
    # pathfilter where more than 1 tag with stockholdersquity and partnercapital are present
    filterd = df[
        df.adsh.isin(result.adsh.tolist()) & df.tag.isin(['StockholdersEquity', 'PartnerCapital'])]
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

    duplicates = df.duplicated(
        ['adsh', 'coreg', 'report', 'uom', 'tag', 'version', 'ddate', 'value'])

    counts_s = df[['adsh', 'report', 'tag']][df.tag.isin(['Assets', 'StockholdersEquity'])].groupby(
        ['adsh', 'report'])['tag'].value_counts()

    new_df = pd.DataFrame(counts_s.to_numpy(), columns=['tag_count'])

    for level in counts_s.index.levels:
        new_df[level.name] = counts_s.index.get_level_values(level.name)

    return new_df


def assets_check(df: pd.DataFrame):
    mask = ~df.Assets.isna() & ~df.AssetsCurrent.isna() & ~df.AssetsNoncurrent.isna()
    df['AssetsCheck_r'] = None
    df['AssetsCheck_cat'] = -1

    df.loc[mask, 'AssetsCheck_r'] = ((df.Assets - df.AssetsCurrent - df.AssetsNoncurrent) / df.Assets).abs()
    df.loc[mask, 'AssetsCheck_cat'] = 3  # gt > 0.1 / 10%
    df.loc[df.AssetsCheck_r < 0.1, 'AssetsCheck_cat'] = 2  # 5-10 %
    df.loc[df.AssetsCheck_r < 0.05, 'AssetsCheck_cat'] = 1  # 1-5 %
    df.loc[df.AssetsCheck_r < 0.01, 'AssetsCheck_cat'] = 0  # < 1%


if __name__ == '__main__':
    print("starting main")
    from secfsdstools.e_collector.reportcollecting import SingleReportCollector
    report = SingleReportCollector.get_report_by_adsh(adsh="0001741257-19-000014")
    standardizer = BalanceSheetStandardizer()
    df_description = standardizer.get_process_description()

    #fs_df = get_fs_from_single_report('0001741257-19-000014')

    fs_df = get_fs_from_all_bs()
    # check_drop_out_mask(fs_df)
    # check_for_equity_tags(fs_df)

    print("fs_df.shape", fs_df.shape)
    print("fs columns", fs_df.columns)

    df = standardizer.process(fs_df)

    # assets_check(df) # adding the AssetsCheck columns
    # print(df.AssetsCheck_cat.value_counts())

    # df_missing_assets = df[df.Assets.isna() | df.AssetsCurrent.isna() | df.AssetsNoncurrent.isna()]

    adshs_set = set(df.adsh.unique().tolist())
    adshs_with_assets = set(df[~df.Assets.isnull()].adsh.unique().tolist())

    missing_assets = adshs_set - adshs_with_assets

    fs_missing_df = df[df.adsh.isin(missing_assets)].copy()

    print(fs_missing_df.groupby('adsh').size().reset_index(name='count'))

    print(len(missing_assets))
    print(len(fs_df.adsh.unique()))
    print(fs_df.columns)

