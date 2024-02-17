import time
from typing import List

import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.companycollecting import CompanyReportCollector
from secfsdstools.e_filter.joinedfiltering import FilterBase
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


@timing
def load_joined_IS_set() -> JoinedDataBag:
    return JoinedDataBag.load("../notebooks/set/parallel/IS/joined")


@timing
def create_smaller_sample_IS_set():
    bag = CompanyReportCollector.get_company_collector(ciks=[789019, 1652044, 1018724],
                                                       stmt_filter=[
                                                           'IS']).collect()  # Microsoft, Alphabet, Amazon
    filtered_bag = bag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][
        USDOnlyRawFilter()]
    filtered_bag.join().save("./saved_data/is_small_joined")


@timing
def load_smaller_sample_IS_set() -> JoinedDataBag:
    return JoinedDataBag.load("./saved_data/is_small_joined")


def filter_tags(pre_num_df: pd.DataFrame, tag_like: str) -> List[str]:
    return [x for x in pre_num_df.tag.unique().tolist() if "Revenue" in x]


@timing
def standardize(is_joined_bag: JoinedDataBag) -> StandardizedBag:
    is_standardizer = IncomeStatementStandardizer()
    is_joined_bag.present(is_standardizer)
    return is_standardizer.get_standardize_bag()


class ISQrtrsFilter(FilterBase):
    """
    Filters the data, so that only datapoints for 4 qrtrs for 10-K,
    and 1 qtrs for 10-Q are kept.
    """

    def filter(self, bag: JoinedDataBag) -> JoinedDataBag:
        # Temporäres DataFrame für "form" hinzufügen
        temp_pre_num_df = pd.merge(bag.pre_num_df, bag.sub_df[['adsh', 'form']], on='adsh', how='inner')

        # Filterkriterien
        criteria = (
                ((temp_pre_num_df['form'] == '10-K') & (temp_pre_num_df['qtrs'] == 4)) |
                ((temp_pre_num_df['form'] == '10-Q') & (temp_pre_num_df['qtrs'] == 1))
        )

        # Ergebnis DataFrame B filtern
        pre_num_df = temp_pre_num_df[criteria]
        del pre_num_df['form']
        return JoinedDataBag.create(sub_df=bag.sub_df, pre_num_df=pre_num_df)


if __name__ == '__main__':
    # create_smaller_sample_IS_set()

    is_joined_bag: JoinedDataBag = load_joined_IS_set()
    # is_joined_bag = load_smaller_sample_IS_set()

    # print(filter_tags(is_joined_bag.pre_num_df, tag_like="Revenue"))
    #
    # # check the loaded data
    print("sub_df", is_joined_bag.sub_df.shape)
    print("pre_num_df", is_joined_bag.pre_num_df.shape)

    is_joined_bag_filtered = is_joined_bag[ISQrtrsFilter()]
    print("filtered pre_num_df", is_joined_bag_filtered.pre_num_df.shape)



    standardized_bag = standardize(is_joined_bag_filtered)

    print(standardized_bag.result_df.shape)

    print("wait")
