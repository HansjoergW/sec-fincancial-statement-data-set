import os

from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.rawfiltering import MainCoregRawFilter, ReportPeriodRawFilter

if __name__ == '__main__':

    def postloadfilter(databag: RawDataBag) -> RawDataBag:
        from secfsdstools.e_filter.rawfiltering import MainCoregRawFilter, ReportPeriodRawFilter
        return databag[ReportPeriodRawFilter()][MainCoregRawFilter()].copy_bag()

    post_filter = lambda x: x[ReportPeriodRawFilter()][MainCoregRawFilter()].copy_bag()
    post_filter_no_copy = lambda x: x[ReportPeriodRawFilter()][MainCoregRawFilter()]

    collector: ZipCollector = ZipCollector.get_all_zips(forms_filter=["10-K", "10-Q"],
                                                        stmt_filter=["BS"],
                                                        post_load_filter=post_filter_no_copy)

    # collector: ZipCollector = ZipCollector.get_zip_by_name(name="2009q4.zip",
    #                                                      forms_filter=["10-K", "10-Q"],
    #                                                      stmt_filter=["BS"],
    #                                                      post_load_filter=post_filter)
    rawdatabag = collector.collect()

    print("sub", rawdatabag.sub_df.shape)

    # just print the size of the pre and num dataframes
    print("pre", rawdatabag.pre_df.shape)
    print("num", rawdatabag.num_df.shape)

    save_path_raw = "./saved_data/raw_bs_10k_10q_all"
    os.makedirs(save_path_raw, exist_ok=True)
    rawdatabag.save(save_path_raw)

    # filtering and joining
    filtered_joined = rawdatabag[MainCoregRawFilter()][ReportPeriodRawFilter()].join()

    print("pre_num", filtered_joined.pre_num_df.shape)

    save_path_joined = "./saved_data/bs_10k_10q_all_joined"
    os.makedirs(save_path_joined, exist_ok=True)
    filtered_joined.save(save_path_joined)
