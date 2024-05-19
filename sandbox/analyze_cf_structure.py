import time
from typing import List

import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.companycollecting import CompanyReportCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, \
    OfficialTagsOnlyRawFilter, USDOnlyRawFilter
from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
from secfsdstools.f_standardize.standardizing import StandardizedBag


def timing(f):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = f(*args, **kwargs)
        time2 = time.time()
        print('{:s} function took {:.3f} ms'.format(f.__name__, (time2 - time1) * 1000.0))

        return ret

    return wrap


def prepare_all_data_set():
    bag = JoinedDataBag.load("../notebooks/set/parallel/CF/joined")
    bag.save("../notebooks/set/filtered/CF/joined")


@timing
def load_joined_CF_set() -> JoinedDataBag:
    return JoinedDataBag.load("../notebooks/set/filtered/CF/joined")


@timing
def create_smaller_sample_CF_set():
    bag = CompanyReportCollector.get_company_collector(ciks=[789019, 1652044, 1018724],
                                                       stmt_filter=[
                                                           'CF']).collect()  # Microsoft, Alphabet, Amazon
    filtered_bag = bag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][
        USDOnlyRawFilter()]
    filtered_bag.join().save("./saved_data/cf_small_joined")


@timing
def load_smaller_sample_IS_set() -> JoinedDataBag:
    return JoinedDataBag.load("./saved_data/is_small_joined")


def filter_tags(pre_num_df: pd.DataFrame, tag_like: str) -> List[str]:
    return [x for x in pre_num_df.tag.unique().tolist() if tag_like in x]


def find_entries_with_all_tags(bag: JoinedDataBag, tag_list: List[str]) -> List[str]:
    filtered_tags_df = bag.pre_num_df[bag.pre_num_df.tag.isin(tag_list)]
    filtered_df = filtered_tags_df[['adsh', 'tag']]
    counted_df = filtered_df.groupby(['adsh']).count().reset_index()
    single_entry = counted_df[counted_df.tag == 1].adsh.tolist()
    single_tags = filtered_df[filtered_df.adsh.isin(single_entry)]

    return counted_df[counted_df.tag == len(tag_list)].adsh.tolist()


def find_entries_with_must_and_others(bag: JoinedDataBag, must_tag: str, others: List[str]):
    prenum_df = bag.pre_num_df
    all_tags = others + [must_tag]
    filtered_must_adshs = prenum_df[prenum_df.tag == must_tag].adsh.tolist()
    filtered_tags_df = prenum_df[
        prenum_df.adsh.isin(filtered_must_adshs) & prenum_df.tag.isin(all_tags)]
    filtered_df = filtered_tags_df[['adsh', 'tag']]
    counted_df = filtered_df.groupby(['adsh']).count().reset_index()
    return counted_df[counted_df.tag > 1].index.tolist()


@timing
def standardize(cf_joined_bag: JoinedDataBag) -> StandardizedBag:
    cf_standardizer = CashFlowStandardizer()
    cf_joined_bag.present(cf_standardizer)
    return cf_standardizer.get_standardize_bag()


def check_signed_values(is_joined_bag: JoinedDataBag, tag_list: List[str]):
    just_cost = is_joined_bag.pre_num_df[['tag', 'value', 'negating']]
    just_cost = just_cost[just_cost.tag.isin(tag_list)]
    just_cost = just_cost[~(just_cost.value.isna() | (just_cost.value == 0.0))]
    just_cost['value_pos'] = just_cost.value >= 0.0
    return just_cost.groupby(['negating', 'value_neg']).count()


if __name__ == '__main__':
    # create_smaller_sample_CF_set()
    # prepare_all_data_set()

    cf_joined_bag: JoinedDataBag = load_joined_CF_set()

    # cf_joined_bag = is_joined_bag.filter(AdshJoinedFilter(adshs=['0001070235-23-000131'])) # expect 2 entries
    # cf_joined_bag = load_smaller_sample_IS_set()

    print("sub_df", cf_joined_bag.sub_df.shape)
    print("pre_num_df", cf_joined_bag.pre_num_df.shape)

    standardized_bag = standardize(cf_joined_bag)

    print(standardized_bag.result_df.shape)

    print("wait")
