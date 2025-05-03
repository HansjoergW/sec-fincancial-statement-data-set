"""
Contains some example code on how to use the different collectors.
"""

import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def singlereportcollector():
    """
    SingleReportCollector example
    """
    from secfsdstools.e_collector.reportcollecting import \
        SingleReportCollector  # pylint: disable=C0415

    apple_10k_2022_adsh = "0000320193-22-000108"

    collector: SingleReportCollector = SingleReportCollector.get_report_by_adsh(
        adsh=apple_10k_2022_adsh)
    rawdatabag = collector.collect()

    # as expected, there is just one entry in the submission dataframe
    print(rawdatabag.sub_df)
    # just print the size of the pre and num dataframes
    print(rawdatabag.pre_df.shape)
    print(rawdatabag.num_df.shape)


def multireportcollector():
    """
    MultiReportCollector example
    """

    from secfsdstools.e_collector.multireportcollecting import \
        MultiReportCollector  # pylint: disable=C0415

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
    # .. and the num_df only contains entries for the Assets tag
    print(rawdatabag.num_df)


def zipcollector():
    """
    ZipCollector example
    """

    from secfsdstools.e_collector.zipcollecting import ZipCollector  # pylint: disable=C0415

    # only collect the Balance Sheet of annual reports that
    # were filed during the first quarter in 2022
    collector: ZipCollector = ZipCollector.get_zip_by_name(name="2022q1.zip",
                                                           forms_filter=["10-K"],
                                                           stmt_filter=["BS"])

    rawdatabag = collector.collect()

    # only show the size of the data frame
    # .. over 4000 companies filed a 10 K report in q1 2022
    print(rawdatabag.sub_df.shape)
    print(rawdatabag.pre_df.shape)
    print(rawdatabag.num_df.shape)


def companyreportcollector():
    """
    CompanyReportCollector example
    """

    from secfsdstools.e_collector.companycollecting import \
        CompanyReportCollector  # pylint: disable=C0415

    apple_cik = 320193
    collector = CompanyReportCollector.get_company_collector(ciks=[apple_cik],
                                                             forms_filter=["10-K"])

    rawdatabag = collector.collect()

    # all filed 10-K reports for apple since 2010 are in the databag
    print(rawdatabag.sub_df)

    print(rawdatabag.pre_df.shape)
    print(rawdatabag.num_df.shape)


def run():
    """launch method"""

    singlereportcollector()
    multireportcollector()
    zipcollector()
    companyreportcollector()


if __name__ == '__main__':
    run()
