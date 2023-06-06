import os
from unittest.mock import MagicMock

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexReport
from secfsdstools.e_read.companyreading import CompanyReader

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_PARQUET = f'{CURRENT_DIR}/testdataparquet/'


def test_get_latest_company_information_parquet():
    reader = CompanyReader(cik=320193, dbaccessor=MagicMock())

    reader.dbaccessor.find_latest_company_report = lambda x: IndexReport(
        adsh='0001193125-10-012085',
        fullPath=f'{PATH_TO_PARQUET}/quarter/2010q1.zip',
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
    print(result)


def test_get_company_reader_parquet():
    reader = CompanyReader.get_company_reader(cik=123,
                                              configuration=Configuration(db_dir="",
                                                                          download_dir="",
                                                                          user_agent_email="",
                                                                          parquet_dir=""))

    assert reader is not None
