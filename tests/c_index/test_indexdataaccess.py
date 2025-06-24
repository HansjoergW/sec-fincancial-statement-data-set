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
    report = IndexReport(
        adsh="abc123",
        cik=1,
        form="10-K",
        name="bla",
        filed=20220130,
        period=20211231,
        originFile="2022q1.zip",
        originFileType="quarter",
        fullPath="",
        url="",
    )

    parquetindexaccessor.insert_indexreport(data=report)

    all_reports: List[IndexReport] = parquetindexaccessor.read_all_indexreports()

    assert len(all_reports) == 1
    assert all_reports[0].adsh == "abc123"

    all_reports_df: pd.DataFrame = parquetindexaccessor.read_all_indexreports_df()
    assert len(all_reports_df) == 1
    assert all_reports_df.iloc[0].adsh == "abc123"


def test_parquetindexprocessing(parquetindexaccessor):
    processing_state = IndexFileProcessingState(
        fileName="2022q1.zip", status="processed", processTime="", fullPath="full", entries=1
    )

    parquetindexaccessor.insert_indexfileprocessing(data=processing_state)

    all_states: List[IndexFileProcessingState] = parquetindexaccessor.read_all_indexfileprocessing()

    assert len(all_states) == 1
    assert all_states[0].fileName == "2022q1.zip"

    all_states_df: pd.DataFrame = parquetindexaccessor.read_all_indexfileprocessing_df()
    assert len(all_states_df) == 1
    assert all_states_df.iloc[0].fileName == "2022q1.zip"


def test_getfilenamesbytype(parquetindexaccessor):
    report = IndexReport(
        adsh="abc123",
        cik=1,
        form="10-K",
        name="bla",
        filed=20220130,
        period=20211231,
        originFile="2022q1.zip",
        originFileType="quarter",
        fullPath="",
        url="",
    )

    parquetindexaccessor.insert_indexreport(data=report)

    names: List[str] = parquetindexaccessor.read_filenames_by_type(originFileType="quarter")
    assert len(names) == 1
    assert names[0] == "2022q1.zip"


def test_get_lastquarter(parquetindexaccessor):
    processing_state1 = IndexFileProcessingState(
        fileName="2022q1.zip", status="processed", processTime="", fullPath="full", entries=1
    )
    processing_state2 = IndexFileProcessingState(
        fileName="2022q2.zip", status="processed", processTime="", fullPath="full", entries=1
    )

    parquetindexaccessor.insert_indexfileprocessing(data=processing_state1)
    parquetindexaccessor.insert_indexfileprocessing(data=processing_state2)

    last_quarter = parquetindexaccessor.find_latest_quarter_file_name()
    assert last_quarter == "2022q2.zip"


def test_clear_index_tables(parquetindexaccessor):
    """Test clearing index tables based on cut_off_day for daily files."""
    # Setup - Create test data for index_parquet_reports table
    reports_to_keep = [
        IndexReport(
            adsh="keep1",
            cik=1,
            form="10-K",
            name="Keep Report 1",
            filed=20220401,
            period=20220331,
            originFile="20220401.zip",
            originFileType="daily",
            fullPath="/path1",
            url="url1",
        ),
        IndexReport(
            adsh="keep2",
            cik=2,
            form="10-Q",
            name="Keep Report 2",
            filed=20220501,
            period=20220430,
            originFile="20220501.zip",
            originFileType="daily",
            fullPath="/path2",
            url="url2",
        ),
        IndexReport(
            adsh="keep3",
            cik=3,
            form="10-K",
            name="Keep Report 3",
            filed=20220401,
            period=20220331,
            originFile="2022q1.zip",
            originFileType="quarter",
            fullPath="/path3",
            url="url3",
        ),
    ]

    reports_to_remove = [
        IndexReport(
            adsh="remove1",
            cik=4,
            form="10-K",
            name="Remove Report 1",
            filed=20220201,
            period=20220131,
            originFile="20220201.zip",
            originFileType="daily",
            fullPath="/path4",
            url="url4",
        ),
        IndexReport(
            adsh="remove2",
            cik=5,
            form="10-Q",
            name="Remove Report 2",
            filed=20220101,
            period=20211231,
            originFile="20220101.zip",
            originFileType="daily",
            fullPath="/path5",
            url="url5",
        ),
    ]

    # Setup - Create test data for index_parquet_processing_state table
    processing_to_keep = [
        IndexFileProcessingState(
            fileName="20220401.zip", status="processed", processTime="2022-04-01", fullPath="/path1", entries=10
        ),
        IndexFileProcessingState(
            fileName="2022q1.zip", status="processed", processTime="2022-04-01", fullPath="/path2", entries=100
        ),
    ]

    processing_to_remove = [
        IndexFileProcessingState(
            fileName="20220201.zip", status="processed", processTime="2022-02-01", fullPath="/path3", entries=5
        ),
        IndexFileProcessingState(
            fileName="20220101.zip", status="processed", processTime="2022-01-01", fullPath="/path4", entries=8
        ),
    ]

    # Insert all test data
    for report in reports_to_keep + reports_to_remove:
        parquetindexaccessor.insert_indexreport(data=report)

    for processing in processing_to_keep + processing_to_remove:
        parquetindexaccessor.insert_indexfileprocessing(data=processing)

    # Verify initial state
    all_reports_before = parquetindexaccessor.read_all_indexreports()
    all_processing_before = parquetindexaccessor.read_all_indexfileprocessing()
    assert len(all_reports_before) == 5
    assert len(all_processing_before) == 4

    # Execution - Clear tables with cut_off_day = 20220300
    cut_off_day = 20220300
    parquetindexaccessor.clear_index_tables(cut_off_day=cut_off_day)

    # Assertion - Verify correct records were removed
    remaining_reports = parquetindexaccessor.read_all_indexreports()
    remaining_processing = parquetindexaccessor.read_all_indexfileprocessing()

    # Should keep 3 reports: 2 daily files after cut_off + 1 quarterly file
    assert len(remaining_reports) == 3
    remaining_adshs = [r.adsh for r in remaining_reports]
    assert "keep1" in remaining_adshs  # Daily file after cut_off
    assert "keep2" in remaining_adshs  # Daily file after cut_off
    assert "keep3" in remaining_adshs  # Quarterly file (not affected by cut_off)
    assert "remove1" not in remaining_adshs  # Daily file before cut_off
    assert "remove2" not in remaining_adshs  # Daily file before cut_off

    # Should keep 2 processing states: 1 daily file after cut_off + 1 quarterly file
    assert len(remaining_processing) == 2
    remaining_filenames = [p.fileName for p in remaining_processing]
    assert "20220401.zip" in remaining_filenames  # Daily file after cut_off
    assert "2022q1.zip" in remaining_filenames  # Quarterly file (different length)
    assert "20220201.zip" not in remaining_filenames  # Daily file before cut_off
    assert "20220101.zip" not in remaining_filenames  # Daily file before cut_off


def test_clear_index_tables_empty_database(parquetindexaccessor):
    """Test clearing index tables when database is empty."""
    # Setup - Verify database is empty
    assert len(parquetindexaccessor.read_all_indexreports()) == 0
    assert len(parquetindexaccessor.read_all_indexfileprocessing()) == 0

    # Execution - Should not raise an error
    cut_off_day = 20220300
    parquetindexaccessor.clear_index_tables(cut_off_day=cut_off_day)

    # Assertion - Database should still be empty
    assert len(parquetindexaccessor.read_all_indexreports()) == 0
    assert len(parquetindexaccessor.read_all_indexfileprocessing()) == 0


def test_clear_index_tables_no_matching_records(parquetindexaccessor):
    """Test clearing index tables when no records match the criteria."""
    # Setup - Create records that should not be removed
    report = IndexReport(
        adsh="keep",
        cik=1,
        form="10-K",
        name="Keep Report",
        filed=20220501,
        period=20220430,
        originFile="20220501.zip",
        originFileType="daily",
        fullPath="/path",
        url="url",
    )
    processing = IndexFileProcessingState(
        fileName="20220501.zip", status="processed", processTime="2022-05-01", fullPath="/path", entries=10
    )

    parquetindexaccessor.insert_indexreport(data=report)
    parquetindexaccessor.insert_indexfileprocessing(data=processing)

    # Execution - Use cut_off_day that won't match any records
    cut_off_day = 20220100  # Earlier than any test data
    parquetindexaccessor.clear_index_tables(cut_off_day=cut_off_day)

    # Assertion - All records should remain
    remaining_reports = parquetindexaccessor.read_all_indexreports()
    remaining_processing = parquetindexaccessor.read_all_indexfileprocessing()

    assert len(remaining_reports) == 1
    assert remaining_reports[0].adsh == "keep"

    assert len(remaining_processing) == 1
    assert remaining_processing[0].fileName == "20220501.zip"


def test_debug_clear_index_tables(parquetindexaccessor):
    """Debug test to understand what's happening with clear_index_tables."""
    # Setup simple test data
    report1 = IndexReport(
        adsh="remove",
        cik=1,
        form="10-K",
        name="Remove",
        filed=20220201,
        period=20220131,
        originFile="20220201.zip",
        originFileType="daily",
        fullPath="/path1",
        url="url1",
    )
    report2 = IndexReport(
        adsh="keep",
        cik=2,
        form="10-K",
        name="Keep",
        filed=20220401,
        period=20220331,
        originFile="20220401.zip",
        originFileType="daily",
        fullPath="/path2",
        url="url2",
    )

    # Setup processing state test data
    processing1 = IndexFileProcessingState(
        fileName="20220201.zip", status="processed", processTime="2022-02-01", fullPath="/path3", entries=5
    )
    processing2 = IndexFileProcessingState(
        fileName="20220401.zip", status="processed", processTime="2022-04-01", fullPath="/path4", entries=10
    )

    parquetindexaccessor.insert_indexreport(data=report1)
    parquetindexaccessor.insert_indexreport(data=report2)
    parquetindexaccessor.insert_indexfileprocessing(data=processing1)
    parquetindexaccessor.insert_indexfileprocessing(data=processing2)

    # Verify initial state
    all_reports = parquetindexaccessor.read_all_indexreports()
    print(f"\nDEBUG: Initial reports: {len(all_reports)}")
    for r in all_reports:
        print(f"  {r.adsh}: {r.originFile}")

    # Test string comparison
    cut_off_file = "20220300.zip"
    print(f"\nDEBUG: String comparisons with cut_off '{cut_off_file}':")
    print(f"  '20220201.zip' < '{cut_off_file}': {'20220201.zip' < cut_off_file}")
    print(f"  '20220401.zip' < '{cut_off_file}': {'20220401.zip' < cut_off_file}")

    # Test the SQL query directly
    with parquetindexaccessor.get_connection() as conn:
        cursor = conn.cursor()

        # Test what records match our condition for reports
        test_sql = "SELECT adsh, originFile FROM index_parquet_reports WHERE originFile < '20220300.zip' and originFileType = 'daily'"
        cursor.execute(test_sql)
        matching_records = cursor.fetchall()
        print(f"\nDEBUG: Reports matching SQL condition: {len(matching_records)}")
        for record in matching_records:
            print(f"  {record[0]}: {record[1]}")

        # Test processing state table conditions
        test_sql3 = "SELECT fileName, length(fileName) FROM index_parquet_processing_state"
        cursor.execute(test_sql3)
        all_processing = cursor.fetchall()
        print(f"\nDEBUG: All processing records with lengths:")
        for record in all_processing:
            print(f"  {record[0]}: length={record[1]}")

        # Test processing state matching condition with correct length
        test_sql4 = "SELECT fileName FROM index_parquet_processing_state WHERE fileName < '20220300.zip' and length(fileName) = 12"
        cursor.execute(test_sql4)
        matching_processing = cursor.fetchall()
        print(f"\nDEBUG: Processing records matching condition (length=12): {len(matching_processing)}")
        for record in matching_processing:
            print(f"  {record[0]}")

    # Execute clear
    cut_off_day = 20220300
    parquetindexaccessor.clear_index_tables(cut_off_day=cut_off_day)

    # Check results
    remaining_reports = parquetindexaccessor.read_all_indexreports()
    remaining_processing = parquetindexaccessor.read_all_indexfileprocessing()

    print(f"\nDEBUG: After clear, remaining reports: {len(remaining_reports)}")
    for r in remaining_reports:
        print(f"  {r.adsh}: {r.originFile}")

    print(f"DEBUG: After clear, remaining processing: {len(remaining_processing)}")
    for p in remaining_processing:
        print(f"  {p.fileName}")

    # The test should pass if only the "keep" records remain
    assert len(remaining_reports) == 1
    assert remaining_reports[0].adsh == "keep"
    assert len(remaining_processing) == 1
    assert remaining_processing[0].fileName == "20220401.zip"
