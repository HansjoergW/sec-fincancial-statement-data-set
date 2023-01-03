import os
from unittest.mock import MagicMock

import pytest

from secfsdstools._1_setup.setupdb import DbCreator
from secfsdstools._2_download.secdownloading import SecZipDownloader
from secfsdstools._3_index.indexdataaccess import IndexFileProcessingState
from secfsdstools._3_index.indexing import ReportZipIndexer


@pytest.fixture
def reportindexer(tmp_path):
    seczipdownloader = SecZipDownloader(zip_dir=str(tmp_path), urldownloader=None)
    seczipdownloader.get_downloaded_list = MagicMock(return_value=['file1', 'file2'])
    DbCreator(db_dir=str(tmp_path)).create_db()
    return ReportZipIndexer(db_dir=str(tmp_path), secdownloader=seczipdownloader)


def test_nothing_indexed(reportindexer):
    not_indexed = reportindexer._calculate_not_indexed()
    assert len(set(not_indexed) - {'file1', 'file2'}) == 0


def test_one_indexed(reportindexer):
    reportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file1", fullPath="", status="processed", processTime=""))

    not_indexed = reportindexer._calculate_not_indexed()
    assert len(set(not_indexed) - {'file2'}) == 0


def test_all_indexed(reportindexer):
    reportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file1", fullPath="", status="processed", processTime=""))

    reportindexer.dbaccessor.insert_indexfileprocessing(
        IndexFileProcessingState(fileName="file2", fullPath="", status="processed", processTime=""))

    not_indexed = reportindexer._calculate_not_indexed()

    assert len(set(not_indexed) - {'file1', 'file2'}) == 0


def test_add_reports(reportindexer):
    current_dir, _ = os.path.split(__file__)

    reportindexer.zip_dir = current_dir + "/testdata/"

    reportindexer._index_file(file_name='2010q1.zip')

    reports_df = reportindexer.dbaccessor.read_all_indexreports_df()

    assert len(reports_df) == 495
