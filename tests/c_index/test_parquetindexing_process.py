import os
from unittest.mock import patch

import pytest

from secfsdstools.b_setup.setupdb import DbCreator
from secfsdstools.c_index.indexdataaccess import IndexFileProcessingState
from secfsdstools.c_index.indexing_process import ReportParquetIndexerProcess, IndexingTask


@pytest.fixture
def parquetreportindexer(tmp_path):
    DbCreator(db_dir=str(tmp_path)).create_db()
    os.makedirs(tmp_path / 'quarter')
    return ReportParquetIndexerProcess(db_dir=str(tmp_path),
                                       parquet_dir=str(tmp_path),
                                       file_type='quarter')


def test_nothing_indexed(parquetreportindexer):
    with patch('secfsdstools.c_index.indexing_process.get_directories_in_directory',
               return_value=['file1', 'file2']):
        not_indexed = parquetreportindexer.calculate_tasks()
        assert len(not_indexed) == 2


def test_one_indexed(parquetreportindexer):
    parquetreportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file1", fullPath="", status="processed", processTime="",
                                 entries=1))
    with patch('secfsdstools.c_index.indexing_process.get_directories_in_directory',
               return_value=['file1', 'file2']):
        not_indexed = parquetreportindexer.calculate_tasks()
        assert len(not_indexed) == 1
        assert not_indexed[0].file_name == "file2"


def test_all_indexed(parquetreportindexer):
    parquetreportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file1", fullPath="", status="processed", processTime="",
                                 entries=1))

    parquetreportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file2", fullPath="", status="processed", processTime="",
                                 entries=1))

    with patch('secfsdstools.c_index.indexing_process.get_directories_in_directory',
               return_value=['file1', 'file2']):
        not_indexed = parquetreportindexer.calculate_tasks()

        assert len(set(not_indexed)) == 0


def test_add_new_reports(parquetreportindexer):
    # test adding a report segments information in num.txt
    # as introduced in January 2025

    current_dir, _ = os.path.split(__file__)

    parquetreportindexer.parquet_dir = f"{current_dir}/../_testdata/parquet_new/"

    file_path = os.path.realpath(os.path.join(parquetreportindexer.parquet_dir,
                                              parquetreportindexer.file_type,
                                              "2010q1.zip"))
    task = IndexingTask(dbaccessor=parquetreportindexer.dbaccessor,
                        file_path=file_path,
                        file_type=parquetreportindexer.file_type,
                        process_time=parquetreportindexer.process_time)

    task.execute()

    reports_df = parquetreportindexer.dbaccessor.read_all_indexreports_df()
    zips_df = parquetreportindexer.dbaccessor.read_all_indexfileprocessing_df()

    assert len(reports_df) == 495
    assert len(zips_df) == 1
