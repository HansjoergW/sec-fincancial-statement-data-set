import time
from typing import List

import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.companycollecting import CompanyReportCollector
from secfsdstools.e_filter.joinedfiltering import FilterBase, AdshJoinedFilter
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, \
    OfficialTagsOnlyRawFilter, USDOnlyRawFilter
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer
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
    bag = JoinedDataBag.load("../notebooks/set/parallel/IS/joined")
    bag = bag[ISQrtrsFilter()]
    bag.save("../notebooks/set/filtered/IS/joinedMultiQtrs")


@timing
def load_joined_IS_set() -> JoinedDataBag:
    return JoinedDataBag.load("../notebooks/set/filtered/IS/joined")

def load_joined_MultiQtrs_IS_set() -> JoinedDataBag:
    return JoinedDataBag.load("../notebooks/set/filtered/IS/joinedMultiQtrs")

@timing
def create_smaller_sample_IS_set():
    bag = CompanyReportCollector.get_company_collector(ciks=[789019, 1652044, 1018724],
                                                       stmt_filter=[
                                                           'IS']).collect()  # Microsoft, Alphabet, Amazon
    filtered_bag = bag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][
        USDOnlyRawFilter()]
    filtered_bag.join()[ISQrtrsFilter()].save("./saved_data/is_small_joined")


@timing
def load_smaller_sample_IS_set() -> JoinedDataBag:
    return JoinedDataBag.load("./saved_data/is_small_joined")


def filter_tags(pre_num_df: pd.DataFrame, tag_like: str) -> List[str]:
    return [x for x in pre_num_df.tag.unique().tolist() if tag_like in x]


def find_entries_with_all_tags(bag: JoinedDataBag, tag_list: List[str]) -> List[str]:
    filtered_tags_df = bag.pre_num_df[bag.pre_num_df.tag.isin(tag_list)]
    filtered_df = filtered_tags_df[['adsh', 'tag']]
    counted_df = filtered_df.groupby(['adsh']).count().reset_index()
    single_entry = counted_df[counted_df.tag==1].adsh.tolist()
    single_tags = filtered_df[filtered_df.adsh.isin(single_entry)]

    return counted_df[counted_df.tag == len(tag_list)].adsh.tolist()

def find_entries_with_must_and_others(bag: JoinedDataBag, must_tag: str, others: List[str]):
    prenum_df = bag.pre_num_df
    all_tags = others + [must_tag]
    filtered_must_adshs = prenum_df[prenum_df.tag==must_tag].adsh.tolist()
    filtered_tags_df = prenum_df[prenum_df.adsh.isin(filtered_must_adshs) & prenum_df.tag.isin(all_tags)]
    filtered_df = filtered_tags_df[['adsh', 'tag']]
    counted_df = filtered_df.groupby(['adsh']).count().reset_index()
    return counted_df[counted_df.tag > 1].index.tolist()

@timing
def standardize(is_joined_bag: JoinedDataBag) -> StandardizedBag:
    is_standardizer = IncomeStatementStandardizer()
    is_joined_bag.present(is_standardizer)
    return is_standardizer.get_standardize_bag()


class ISQrtrsFilter(FilterBase):
    """
    Filters the data, so that only datapoints for 4 qtrs for 10-K,
    and 1 qtrs for 10-Q are kept.
    """

    def filter(self, bag: JoinedDataBag) -> JoinedDataBag:
        # Temporäres DataFrame für "form" hinzufügen
        temp_pre_num_df = pd.merge(bag.pre_num_df, bag.sub_df[['adsh', 'form']], on='adsh',
                                   how='inner')

        # Filterkriterien
        criteria = (
                ((temp_pre_num_df['form'] == '10-K') & (temp_pre_num_df['qtrs'] == 4)) |
                ((temp_pre_num_df['form'] == '10-Q') & (temp_pre_num_df['qtrs'] < 4))
        )

        # Ergebnis DataFrame B filtern
        pre_num_df = temp_pre_num_df[criteria]
        del pre_num_df['form']
        return JoinedDataBag.create(sub_df=bag.sub_df, pre_num_df=pre_num_df)


def check_signed_values(is_joined_bag: JoinedDataBag, tag_list: List[str]):

    just_cost = is_joined_bag.pre_num_df[['tag', 'value', 'negating']]
    just_cost = just_cost[just_cost.tag.isin(tag_list)]
    just_cost = just_cost[~(just_cost.value.isna() | (just_cost.value == 0.0))]
    just_cost['value_pos'] = just_cost.value >= 0.0
    return just_cost.groupby(['negating', 'value_neg']).count()



def find_operating_expense_tags(joined_bag: JoinedDataBag):
    adshs = find_entries_with_all_tags(joined_bag, tag_list=['GrossProfit', 'OperatingIncomeLoss'])
    print(len(adshs))
    relevant = joined_bag.pre_num_df[joined_bag.pre_num_df.adsh.isin(adshs)][['adsh','tag','line']]

    tag_counts = pd.Series(dtype=int)

    for adsh, group in relevant.groupby('adsh'):
        line_gp_lst = group[group.tag=='GrossProfit'].line.tolist()
        if len(line_gp_lst) == 0:
            continue

        line_gp = line_gp_lst[0]

        line_oil_list = group[group.tag=='OperatingIncomeLoss'].line.tolist()
        if len(line_oil_list) == 0:
            continue

        line_oil = line_oil_list[0]
        relevant_tags = group[(group.line > line_gp) & (group.line<line_oil)].tag
        # Aktualisieren der Tag-Zählungen für jede Gruppe
        tag_counts = tag_counts.add(relevant_tags.value_counts(), fill_value=0)

    print(tag_counts)



if __name__ == '__main__':
    # create_smaller_sample_IS_set()
    # prepare_all_data_set()

    is_joined_bag: JoinedDataBag = load_joined_MultiQtrs_IS_set()
    from secfsdstools.u_usecases.analyzes import find_tags_containing
    # with_profitloss = find_tags_containing(is_joined_bag, 'ProfitLoss')
    # print(with_profitloss)

    #find_operating_expense_tags(is_joined_bag)
    # print(check_signed_values(is_joined_bag, tag_list=['LicenseCost',
    #                                              'CostOfRevenue',
    #                                              'CostOfGoodsAndServicesSold',
    #                                              'CostOfGoodsSold',
    #                                              'CostOfServices']))


    #is_joined_bag = is_joined_bag.filter(AdshJoinedFilter(adshs=['0001070235-23-000131'])) # expect 2 entries

    #is_joined_bag = load_smaller_sample_IS_set()
    #


    # print(find_entries_with_all_tags(bag=is_joined_bag,
    #                            tag_list=[
    #                                'IncomeLossBeforeIncomeTaxExpenseBenefit',
    #                                'IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
    #                            ]))

    # print(find_entries_with_must_and_others(
    #     bag=is_joined_bag,
    #     must_tag='SalesRevenueGoodsGross',
    #     others=['SalesRevenueGoodsNet',
    #             'SalesRevenueServicesNet']
    # ))

    #print(filter_tags(is_joined_bag.pre_num_df, tag_like="IncomeLossFromDiscontinuedOperations"))
    #
    # # check the loaded data
    print("sub_df", is_joined_bag.sub_df.shape)
    print("pre_num_df", is_joined_bag.pre_num_df.shape)

    #cost_of_lst = is_joined_bag.pre_num_df[is_joined_bag.pre_num_df.tag.str.contains('CostOf')].tag.unique().tolist()


    standardized_bag = standardize(is_joined_bag)

    print(standardized_bag.result_df.shape)

    print("wait")
