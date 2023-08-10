import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def singlereportcollector():
    from secfsdstools.e_collector.reportcollecting import SingleReportCollector

    apple_10k_2022_adsh = "0000320193-22-000108"

    collector: SingleReportCollector = SingleReportCollector.get_report_by_adsh(adsh=apple_10k_2022_adsh)
    rawdatabag = collector.collect()

    # as expected, there is just one entry in the submission dataframe
    print(rawdatabag.sub_df)
    # just print the size of the pre and num dataframes
    print(rawdatabag.pre_df.shape)
    print(rawdatabag.num_df.shape)


def multireportcollector():
    from secfsdstools.e_collector.multireportcollecting import MultiReportCollector
    apple_10k_2022_adsh = "0000320193-22-000108"
    apple_10k_2012_adsh = "0001193125-12-444068"

    # load only the assets tags that are present in the 10-K report of apple in the years
    # 2022 and 2012
    collector: MultiReportCollector = \
        MultiReportCollector.get_reports_by_adshs(adshs=[apple_10k_2022_adsh,
                                                         apple_10k_2012_adsh],
                                                  tag_filter=['Assets'])
    rawdatabag = collector.collect()
    # as expected, there are just two entries in the submission dataframe
    print(rawdatabag.sub_df)
    print(rawdatabag.num_df)


if __name__ == '__main__':
    singlereportcollector()
    multireportcollector()