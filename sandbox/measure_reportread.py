import time

from secfsdstools.d_index.indexdataaccess import IndexReport
from secfsdstools.e_read.basereportreading import NUM_TXT, PRE_TXT, SUB_TXT
from secfsdstools.e_read.reportreading import ReportReader

if __name__ == '__main__':
    report = IndexReport(adsh='', fullPath='./../data/dld/2022q4.zip', cik=0, name='', originFile='',
                         originFileType='', url='', filed=0, form='', period=0)

    reader = ReportReader.get_report_by_indexreport(index_report=report)
    start_time = time.time()

    for _ in range(10):
        reader.num_df = None
        reader._read_df_from_raw(NUM_TXT)
        reader._read_df_from_raw(PRE_TXT)
        reader._read_df_from_raw(SUB_TXT)

    end_time = time.time()
    dauer = end_time - start_time
    print(dauer)