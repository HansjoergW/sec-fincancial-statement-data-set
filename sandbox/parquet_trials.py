# from secfsdstools.a_utils.fileutils import read_df_from_file_in_zip
# from secfsdstools.e_read.basereportreading import NUM_TXT, PRE_TXT, SUB_TXT

import time

import pandas as pd

from secfsdstools.a_utils.fileutils import read_df_from_file_in_zip
from secfsdstools.e_read.basereportreading import NUM_TXT, PRE_TXT, SUB_TXT

if __name__ == '__main__':

    test_zip = '../data/dld/2022q4.zip'
    parquet_folder = '../data/parquet/'
    parquet_file_num = f'{parquet_folder}2022q4_num'
    parquet_file_pre = f'{parquet_folder}2022q4_pre'
    parquet_file_sub = f'{parquet_folder}2022q4_sub'

    # num_txt_df = read_df_from_file_in_zip(test_zip, NUM_TXT)
    # pre_txt_df = read_df_from_file_in_zip(test_zip, PRE_TXT)
    # sub_txt_df = read_df_from_file_in_zip(test_zip, SUB_TXT)
    #
    # num_txt_df.to_parquet(parquet_file_num)
    # pre_txt_df.to_parquet(parquet_file_pre)
    # sub_txt_df.to_parquet(parquet_file_sub)

    adsh = '0000047217-22-000068'

    start_time = time.time()
    for i in range(10):
        print(i)
        pd.read_parquet(parquet_file_num, filters=[('adsh', '==', adsh)])
        pd.read_parquet(parquet_file_pre, filters=[('adsh', '==', adsh)])
        pd.read_parquet(parquet_file_sub, filters=[('adsh', '==', adsh)])

    end_time = time.time()
    dauer = end_time - start_time
    print(dauer)

    # 5.4
    # hp = pd.read_parquet(parquet_file, filters=[('adsh', '==', '0000047217-22-000068')])
    # print(hp)
    # num_txt_df.to_parquet(f'{parquet_folder}2022q3_num_txt_2', partition_cols=['adsh'])
