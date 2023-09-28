import os

from secfsdstools.c_index.companyindexreading import CompanyIndexReader
from secfsdstools.c_index.indexdataaccess import IndexReport

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_PARQUET = f'{CURRENT_DIR}/../_testdata/parquet/'


def test_get_latest_company_information_parquet(basicconf):
    reader = CompanyIndexReader.get_company_index_reader(cik=320193, configuration=basicconf)

    reader.dbaccessor.find_latest_company_report = lambda x: IndexReport(
        adsh='0001193125-10-012085',
        fullPath=f'{basicconf.parquet_dir}/quarter/2010q1.zip',
        cik=320193,
        name='',
        form='',
        filed=0,
        period=0,
        originFile='',
        originFileType='',
        url='',
    )

    result = reader.get_latest_company_filing()
    assert result is not None
    assert result['adsh'] == '0001193125-10-012085'
    assert result['cik'] == 320193
    assert result['name'] == 'APPLE INC'
    assert result['form'] == '10-Q'
    assert result['period'] == 20091231
