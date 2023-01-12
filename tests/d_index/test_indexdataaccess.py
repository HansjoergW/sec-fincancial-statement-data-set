from typing import List

import pandas as pd
import pytest

from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.d_index.indexdataaccess import DBIndexingAccessor, IndexReport, IndexFileProcessingState


@pytest.fixture
def indexaccessor(tmp_path):
    DbCreator(db_dir=str(tmp_path)).create_db()
    accessor = DBIndexingAccessor(db_dir=str(tmp_path))
    return accessor


def test_indexreports(indexaccessor):
    report = IndexReport(adsh='abc123', cik=1, form='10-K', name='bla', filed=20220130, period=20211231,
                         originFile='2022q1.zip', originFileType='quarter', fullPath='', url='')

    indexaccessor.insert_indexreport(data=report)

    all_reports: List[IndexReport] = indexaccessor.read_all_indexreports()

    assert len(all_reports) == 1
    assert all_reports[0].adsh == 'abc123'

    all_reports_df: pd.DataFrame = indexaccessor.read_all_indexreports_df()
    assert len(all_reports_df) == 1
    assert all_reports_df.iloc[0].adsh == 'abc123'


def test_indexprocessing(indexaccessor):
    processing_state = IndexFileProcessingState(fileName='2022q1.zip', status='processed',
                                                processTime='', fullPath='full', entries=1)

    indexaccessor.insert_indexfileprocessing(data=processing_state)

    all_states: List[IndexFileProcessingState] = indexaccessor.read_all_indexfileprocessing()

    assert len(all_states) == 1
    assert all_states[0].fileName == '2022q1.zip'

    all_states_df: pd.DataFrame = indexaccessor.read_all_indexfileprocessing_df()
    assert len(all_states_df) == 1
    assert all_states_df.iloc[0].fileName == '2022q1.zip'
