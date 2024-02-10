import time

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_collector.companycollecting import CompanyReportCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, \
    OfficialTagsOnlyRawFilter, USDOnlyRawFilter
from secfsdstools.f_standardize.standardizing import StandardizedBag
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer


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


@timing
def standardize(is_joined_bag: JoinedDataBag) -> StandardizedBag:
    is_standardizer = IncomeStatementStandardizer()
    is_joined_bag.present(is_standardizer)
    return is_standardizer.get_standardize_bag()


if __name__ == '__main__':
    # create_smaller_sample_IS_set()
    # is_joined_bag = load_joined_IS_set()

    is_joined_bag = load_smaller_sample_IS_set()
    #
    # # check the loaded data
    # print(is_joined_bag.sub_df.shape)
    #
    # standardized_bag = standardize(is_joined_bag)
    #
    # print(standardized_bag.result_df.shape)
