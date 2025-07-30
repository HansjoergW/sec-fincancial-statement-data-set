"""
Code to check whether the combined datasets contain the right amount of data.
created by the memory_optimized_daily_automation.py script.
"""

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.f_standardize.standardizing import StandardizedBag

quarter_path_joined_by_stmt_bs: str = "c:/data/sec/automated/_2_all_quarter/_1_joined_by_stmt/BS"
quarter_path_joined: str = "c:/data/sec/automated/_2_all_quarter/_2_joined"
quarter_path_standardized_by_stmt: str = "c:/data/sec/automated/_2_all_quarter/_3_standardized_by_stmt/BS"

daily_path_joined_by_stmt_bs: str = "c:/data/sec/automated/_2_all_day/_1_joined_by_stmt/BS"
daily_path_joined: str = "c:/data/sec/automated/_2_all_day/_2_joined"
daily_path_standardized_by_stmt: str = "c:/data/sec/automated/_2_all_day/_3_standardized_by_stmt/BS"

all_path_joined_by_stmt_bs: str = "c:/data/sec/automated/_3_all/_1_joined_by_stmt/BS"
all_path_joined: str = "c:/data/sec/automated/_3_all/_2_joined"
all_path_standardized_by_stmt: str = "c:/data/sec/automated/_3_all/_3_standardized_by_stmt/BS"


def check_joined_by_stmt():
    quarter_bag = JoinedDataBag.load(quarter_path_joined_by_stmt_bs)
    daily_bag = JoinedDataBag.load(daily_path_joined_by_stmt_bs)
    all_bag = JoinedDataBag.load(all_path_joined_by_stmt_bs)

    print("quarter_bag", quarter_bag.sub_df.shape)
    print("daily_bag", daily_bag.sub_df.shape)
    print("all_bag", all_bag.sub_df.shape)

    print("check counts: ", quarter_bag.sub_df.shape[0] + daily_bag.sub_df.shape[0] == all_bag.sub_df.shape[0])

    print(
        "check adshs: ",
        set(quarter_bag.sub_df.adsh.tolist() + daily_bag.sub_df.adsh.tolist()) == set(all_bag.sub_df.adsh.tolist()),
    )


def check_joined():
    quarter_bag = JoinedDataBag.load(quarter_path_joined)
    daily_bag = JoinedDataBag.load(daily_path_joined)
    all_bag = JoinedDataBag.load(all_path_joined)

    print("quarter_bag", quarter_bag.sub_df.shape)
    print("daily_bag", daily_bag.sub_df.shape)
    print("all_bag", all_bag.sub_df.shape)

    print("check counts: ", quarter_bag.sub_df.shape[0] + daily_bag.sub_df.shape[0] == all_bag.sub_df.shape[0])
    print(
        "check adshs: ",
        set(quarter_bag.sub_df.adsh.tolist() + daily_bag.sub_df.adsh.tolist()) == set(all_bag.sub_df.adsh.tolist()),
    )


def check_standardized_by_stmt():
    quarter_bag = StandardizedBag.load(quarter_path_standardized_by_stmt)
    daily_bag = StandardizedBag.load(daily_path_standardized_by_stmt)
    all_bag = StandardizedBag.load(all_path_standardized_by_stmt)

    print("quarter_bag", quarter_bag.result_df.shape)
    print("daily_bag", daily_bag.result_df.shape)
    print("all_bag", all_bag.result_df.shape)

    print("check counts: ", quarter_bag.result_df.shape[0] + daily_bag.result_df.shape[0] == all_bag.result_df.shape[0])
    print(
        "check adshs: ",
        set(quarter_bag.result_df.adsh.tolist() + daily_bag.result_df.adsh.tolist())
        == set(all_bag.result_df.adsh.tolist()),
    )


if __name__ == "__main__":
    # check_joined_by_stmt()
    # check_joined()
    check_standardized_by_stmt()
