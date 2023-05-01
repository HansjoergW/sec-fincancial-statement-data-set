import os
from unittest.mock import patch

import pytest

from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_index.indexdataaccess import IndexFileProcessingState
from secfsdstools.c_index.indexing import ReportZipIndexer


@pytest.fixture
def zipreportindexer(tmp_path):
    DbCreator(db_dir=str(tmp_path)).create_db()
    return ReportZipIndexer(db_dir=str(tmp_path), zip_dir=str(tmp_path), file_type='quarter')


def test_nothing_indexed(zipreportindexer):
    with patch('secfsdstools.c_index.indexing.get_filenames_in_directory',
               return_value=['file1', 'file2']):
        not_indexed = zipreportindexer._calculate_not_indexed()
        assert len(set(not_indexed) - {'file1', 'file2'}) == 0


def test_one_indexed(zipreportindexer):
    zipreportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file1", fullPath="", status="processed", processTime="",
                                 entries=1))

    not_indexed = zipreportindexer._calculate_not_indexed()
    assert len(set(not_indexed) - {'file2'}) == 0


def test_all_indexed(zipreportindexer):
    zipreportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file1", fullPath="", status="processed", processTime="",
                                 entries=1))

    zipreportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file2", fullPath="", status="processed", processTime="",
                                 entries=1))

    not_indexed = zipreportindexer._calculate_not_indexed()

    assert len(set(not_indexed) - {'file1', 'file2'}) == 0


def test_add_reports(zipreportindexer):
    current_dir, _ = os.path.split(__file__)

    zipreportindexer.zip_dir = f"{current_dir}/testdata/"

    zipreportindexer._index_file(file_name='2010q1.zip')

    reports_df = zipreportindexer.dbaccessor.read_all_indexreports_df()

    assert len(reports_df) == 495
