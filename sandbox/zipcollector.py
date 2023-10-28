from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregFilter


if __name__ == '__main__':

    def postloadfilter(databag: RawDataBag) -> RawDataBag:
        from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregFilter
        return databag[ReportPeriodRawFilter()][MainCoregFilter()].copy_bag()

    post_filter = lambda x: x[ReportPeriodRawFilter()][MainCoregFilter()].copy_bag()
    post_filter_no_copy = lambda x: x[ReportPeriodRawFilter()][MainCoregFilter()]

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

    # joining the pre and num dataframes
    joineddatabag = rawdatabag.join()
    print("pre_num", joineddatabag.pre_num_df.shape)
