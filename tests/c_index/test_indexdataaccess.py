from typing import List

import pandas as pd
import pytest
from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_index.indexdataaccess import IndexFileProcessingState, IndexReport, ParquetDBIndexingAccessor


@pytest.fixture
def parquetindexaccessor(tmp_path) -> ParquetDBIndexingAccessor:
    DbCreator(db_dir=str(tmp_path)).create_db()
    return ParquetDBIndexingAccessor(db_dir=str(tmp_path))


def test_parquetindexreports(parquetindexaccessor):
    report = IndexReport(adsh='abc123', cik=1, form='10-K', name='bla', filed=20220130,
                         period=20211231,
                         originFile='2022q1.zip', originFileType='quarter', fullPath='', url='')

    parquetindexaccessor.insert_indexreport(data=report)

    all_reports: List[IndexReport] = parquetindexaccessor.read_all_indexreports()

    assert len(all_reports) == 1
    assert all_reports[0].adsh == 'abc123'

    all_reports_df: pd.DataFrame = parquetindexaccessor.read_all_indexreports_df()
    assert len(all_reports_df) == 1
    assert all_reports_df.iloc[0].adsh == 'abc123'


def test_parquetindexprocessing(parquetindexaccessor):
    processing_state = IndexFileProcessingState(fileName='2022q1.zip', status='processed',
                                                processTime='', fullPath='full', entries=1)

    parquetindexaccessor.insert_indexfileprocessing(data=processing_state)

    all_states: List[IndexFileProcessingState] = parquetindexaccessor.read_all_indexfileprocessing()

    assert len(all_states) == 1
    assert all_states[0].fileName == '2022q1.zip'

    all_states_df: pd.DataFrame = parquetindexaccessor.read_all_indexfileprocessing_df()
    assert len(all_states_df) == 1
    assert all_states_df.iloc[0].fileName == '2022q1.zip'


def test_getfilenamesbytype(parquetindexaccessor):
    report = IndexReport(adsh='abc123', cik=1, form='10-K', name='bla', filed=20220130,
                         period=20211231,
                         originFile='2022q1.zip', originFileType='quarter', fullPath='', url='')

    parquetindexaccessor.insert_indexreport(data=report)

    names: List[str] = parquetindexaccessor.read_filenames_by_type(originFileType="quarter")
    assert len(names) == 1
    assert names[0] == "2022q1.zip"
