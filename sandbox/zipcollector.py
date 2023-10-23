from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.d_container.filter import FilterBase
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregFilter


class MyFilter(FilterBase[RawDataBag]):

    def filter(self, databag: RawDataBag) -> RawDataBag:
        return databag[ReportPeriodRawFilter()][MainCoregFilter()].copy_bag()


if __name__ == '__main__':
    collector: ZipCollector = ZipCollector.get_all_zips(forms_filter=["10-K", "10-Q"],
                                                        stmt_filter=["BS"],
                                                        post_load_filter=MyFilter())

    # collector: ZipCollector = ZipCollector.get_zip_by_name(name="2022q1.zip",
    #                                                      forms_filter=["10-K", "10-Q"],
    #                                                      post_load_filter=MyFilter())
    rawdatabag = collector.collect()

    print("sub", rawdatabag.sub_df.shape)

    # just print the size of the pre and num dataframes
    print("pre", rawdatabag.pre_df.shape)
    print("num", rawdatabag.num_df.shape)

    # joining the pre and num dataframes
    joineddatabag = rawdatabag.join()
    print("pre_num", joineddatabag.pre_num_df.shape)
