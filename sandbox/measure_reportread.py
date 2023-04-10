import time

from secfsdstools.a_config.configmgt import Configuration
from secfsdstools.d_index.indexdataaccess import IndexReport
from secfsdstools.e_read.basereportreading import NUM_TXT, PRE_TXT, SUB_TXT
from secfsdstools.e_read.companycollecting import CompanyReportCollector
from secfsdstools.e_read.reportreading import ReportReader


def measure_report_read():
    # zip: 95
    # parquet: 0.2
    report_zip = IndexReport(adsh='', fullPath='./../data/dld/2022q4.zip', cik=0, name='',
                             originFile='',
                             originFileType='', url='', filed=0, form='', period=0)

    report_parquet = IndexReport(adsh='', fullPath='./../data/parquet/quarter/2022q4.zip', cik=0,
                                 name='', originFile='',
                                 originFileType='', url='', filed=0, form='', period=0)

    reader = ReportReader.get_report_by_indexreport(index_report=report_zip)
    start_time = time.time()

    for counter in range(10):
        print(counter)
        reader.num_df = None
        reader._read_df_from_raw(NUM_TXT)
        reader._read_df_from_raw(PRE_TXT)
        reader._read_df_from_raw(SUB_TXT)

    end_time = time.time()
    dauer = end_time - start_time
    print(dauer)


def measure_company_collector():
    # zip: 94
    # parquet: 22 sec
    apple_cik = 320193
    config = Configuration(db_dir="./../data/db",
                           parquet_dir="./../data/parquet",
                           download_dir="./../data/dld",
                           user_agent_email="hj@mycomp.com",
                           use_parquet=False
                           )
    start_time = time.time()

    for counter in range(5):
        print(counter)
        collector = CompanyReportCollector.get_company_collector(cik=apple_cik,
                                                                 configuration=config,
                                                                 forms=['10-K'])
        collector._read_raw_data()
    end_time = time.time()
    dauer = end_time - start_time
    print(dauer)


if __name__ == '__main__':
    measure_company_collector()
