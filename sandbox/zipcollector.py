from secfsdstools.e_collector.zipcollecting import ZipCollector


if __name__ == '__main__':
    collector: ZipCollector = ZipCollector.get_all_zips(forms_filter=["10-K", "10-Q"],
                                                        tag_filter=["Assets"])

    # collector: ZipCollector = ZipCollector.get_zip_by_name(name="2009q1.zip",
    #                                                     forms_filter=["10-K", "10-Q"],
    #                                                     tag_filter=["Assets"])
    rawdatabag = collector.collect()

    print("sub", rawdatabag.sub_df.shape)

    # just print the size of the pre and num dataframes
    print("pre", rawdatabag.pre_df.shape)
    print("num", rawdatabag.num_df.shape)

    # joining the pre and num dataframes
    joineddatabag = rawdatabag.join()
    print("pre_num", joineddatabag.pre_num_df.shape)