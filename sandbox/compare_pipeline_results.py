"""
Compares the old and new runs of the pipeline automation.
"""
from pathlib import Path

import pyarrow.parquet as pq

PATH_OLD = Path("c:/data/sec/automated/old_4_single_bag/all/")
PATH_NEW = Path("c:/data/sec/automated/_4_single_bag/all/")

if __name__ == '__main__':
    sub_new = pq.ParquetFile(PATH_NEW / "sub.txt.parquet")
    sub_old = pq.ParquetFile(PATH_OLD / "sub.txt.parquet")

    pre_num_new = pq.ParquetFile(PATH_NEW / "pre_num.txt.parquet")
    pre_num_old = pq.ParquetFile(PATH_OLD / "pre_num.txt.parquet")

    print("sub rows new: ", sub_new.metadata.num_rows)
    print("sub rows old: ", sub_old.metadata.num_rows)

    print("pre_nun rows new: ", pre_num_new.metadata.num_rows)
    print("pre_num rows old: ", pre_num_old.metadata.num_rows)
