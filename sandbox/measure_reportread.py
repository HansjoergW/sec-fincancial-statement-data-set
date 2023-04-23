import time

from secfsdstools.a_config.configmgt import Configuration
from secfsdstools.d_index.indexdataaccess import IndexReport
from secfsdstools.d_index.indexing import BaseReportIndexer
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


def measure_single_adsh():
    # zip: 6 sec
    # parquet: 0.5 sec
    adsh = '0000320193-22-000108'
    invert_negated = True
    stmts = ['BS', 'CF', 'IS']
    rows = 10

    config = Configuration(db_dir="./../data/db",
                           parquet_dir="./../data/parquet",
                           download_dir="./../data/dld",
                           user_agent_email="hj@mycomp.com",
                           use_parquet=False
                           )

    start_time = time.time()
    reader = ReportReader.get_report_by_adsh(adsh=adsh,
                                             configuration=config)
    reader._read_raw_data()

    read_time = time.time() - start_time

    # get some key infos of the report
    submission_data = {k: v for k, v in reader.submission_data().items() if
                       k in ['cik', 'adsh', 'name', 'cityba', 'form', 'period', 'filed']}

    report_data = reader.merge_pre_and_num()
    merge_time = time.time() - start_time

    # we only look at data for the submitted period, meaning we don't show data from the previous period
    report_data = report_data[report_data.ddate == submission_data['period']]

    # sort the data in a meaningful way
    report_data = report_data.sort_values(['adsh', 'coreg', 'ddate', 'stmt', 'report', 'line', ])

    # filter for the selected statements
    report_data = report_data[report_data.stmt.isin(stmts)]

    if invert_negated:
        report_data.loc[report_data.negating == 1, 'value'] = -report_data.value

    filter_time = time.time() - start_time

    # use a meaningful column order
    report_data = report_data[
        ['adsh', 'coreg', 'ddate', 'stmt', 'report', 'line', 'tag', 'version', 'uom', 'value',
         'negating', 'plabel', 'qtrs', 'footnote', 'inpth', 'rfile']]

    # create and display the url on which the report is published on sec.gov, so that it can directly be opened
    url = BaseReportIndexer.URL_PREFIX + str(submission_data['cik']) + '/' + submission_data[
        'adsh'].replace('-', '') + '/' + submission_data['adsh'] + '-index.htm'
    ready_time = time.time() - start_time
    print(url)

    # display the key submission data of the report
    print(submission_data)

    # display the data of the report
    print(report_data.head(rows))

    end_time = time.time() - start_time

    print(read_time)
    print(merge_time)
    print(filter_time)
    print(ready_time)
    print(end_time)


if __name__ == '__main__':
    measure_single_adsh()
