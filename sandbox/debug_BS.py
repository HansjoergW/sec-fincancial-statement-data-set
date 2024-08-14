from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer


def run_test():
    all_bs_joinedbag: JoinedDataBag = JoinedDataBag.load("../notebooks/set/serial/BS/joined")

    # standardize the data
    bs_standardizer = BalanceSheetStandardizer()
    all_bs_joinedbag.present(bs_standardizer)


if __name__ == '__main__':
    run_test()
