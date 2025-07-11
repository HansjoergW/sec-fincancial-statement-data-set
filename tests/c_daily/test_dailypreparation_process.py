"""
Tests for the DailyPreparationProcess class.

Tests cover static methods for quarter calculations and instance methods
for clearing index tables and daily parquet files.
"""

from unittest.mock import Mock, patch

from secdaily._00_common.BaseDefinitions import QuarterInfo

from secfsdstools.c_daily.dailypreparation_process import DailyPreparationProcess


def test_calculate_daily_start_quarter_regular_quarter():
    """Test calculating the next quarter when it's not the 4th quarter."""
    # Test Q1 -> Q2
    result = DailyPreparationProcess._calculate_daily_start_quarter("2022q1")
    assert result.year == 2022
    assert result.qrtr == 2

    # Test Q2 -> Q3
    result = DailyPreparationProcess._calculate_daily_start_quarter("2022q2")
    assert result.year == 2022
    assert result.qrtr == 3

    # Test Q3 -> Q4
    result = DailyPreparationProcess._calculate_daily_start_quarter("2022q3")
    assert result.year == 2022
    assert result.qrtr == 4


def test_calculate_daily_start_quarter_year_transition():
    """Test calculating the next quarter when transitioning from Q4 to Q1 of next year."""
    # Test Q4 -> Q1 of next year
    result = DailyPreparationProcess._calculate_daily_start_quarter("2022q4")
    assert result.year == 2023
    assert result.qrtr == 1


def test_cut_off_day():
    """Test the cut_off_day calculation for different quarters."""
    # Create QuarterInfo objects for each quarter
    q1 = QuarterInfo(2022, 1)
    q2 = QuarterInfo(2022, 2)
    q3 = QuarterInfo(2022, 3)
    q4 = QuarterInfo(2022, 4)

    # Test cut_off_day for each quarter
    assert DailyPreparationProcess._cut_off_day(q1) == 20220000  # Q1: yyyy0000 Everyting in the past years
    assert DailyPreparationProcess._cut_off_day(q2) == 20220400  # Q2: yyyy0400 Everything before april
    assert DailyPreparationProcess._cut_off_day(q3) == 20220700  # Q3: yyyy0700 Everything before july
    assert DailyPreparationProcess._cut_off_day(q4) == 20221000  # Q4: yyyy1000 Everything before october


def test_clear_index_tables():
    """Test clearing index tables with a specific cut_off_day."""
    # Setup
    mock_index_accessor = Mock()

    with patch("secfsdstools.c_daily.dailypreparation_process.ParquetDBIndexingAccessor") as mock_accessor_class:
        mock_accessor_class.return_value = mock_index_accessor

        process = DailyPreparationProcess(db_dir="/test/db", parquet_dir="/test/parquet", daily_dir="/test/daily")

        # Execution
        cut_off_day = 20220300
        process.clear_index_tables(cut_off_day=cut_off_day)

        # Assertion
        mock_index_accessor.clear_index_tables.assert_called_once_with(cut_off_day=20220300)


def test_clear_daily_parquet_files_directory_exists(tmp_path):
    """Test clearing daily parquet files when directory exists with files to remove."""
    # Setup
    parquet_dir = tmp_path / "parquet"
    daily_dir = parquet_dir / "daily"
    daily_dir.mkdir(parents=True)

    # Create test directories that should be removed (before cut_off)
    (daily_dir / "20220101.zip").mkdir()
    (daily_dir / "20220201.zip").mkdir()

    # Create test directories that should remain (after cut_off)
    (daily_dir / "20220401.zip").mkdir()
    (daily_dir / "20220501.zip").mkdir()

    # Create a file (not directory) that should be ignored
    (daily_dir / "somefile.txt").touch()

    mock_index_accessor = Mock()

    with patch("secfsdstools.c_daily.dailypreparation_process.ParquetDBIndexingAccessor") as mock_accessor_class:
        mock_accessor_class.return_value = mock_index_accessor

        process = DailyPreparationProcess(db_dir="/test/db", parquet_dir=str(parquet_dir), daily_dir="/test/daily")

        # Execution
        cut_off_day = 20220300
        process.clear_daily_parquet_files(cut_off_day=cut_off_day)

        # Assertion
        remaining_dirs = [d.name for d in daily_dir.iterdir() if d.is_dir()]
        remaining_files = [f.name for f in daily_dir.iterdir() if f.is_file()]

        # Directories before cut_off should be removed
        assert "20220101.zip" not in remaining_dirs
        assert "20220201.zip" not in remaining_dirs

        # Directories after cut_off should remain
        assert "20220401.zip" in remaining_dirs
        assert "20220501.zip" in remaining_dirs

        # Files should be ignored and remain
        assert "somefile.txt" in remaining_files


def test_clear_daily_parquet_files_directory_not_exists():
    """Test clearing daily parquet files when directory does not exist."""
    # Setup
    mock_index_accessor = Mock()

    with patch("secfsdstools.c_daily.dailypreparation_process.ParquetDBIndexingAccessor") as mock_accessor_class:
        mock_accessor_class.return_value = mock_index_accessor

        process = DailyPreparationProcess(
            db_dir="/test/db", parquet_dir="/nonexistent/parquet", daily_dir="/test/daily"
        )

        # Execution - should not raise an error
        cut_off_day = 20220300
        process.clear_daily_parquet_files(cut_off_day=cut_off_day)

        # Assertion - method should complete without error
        # No specific assertions needed as we're testing it doesn't crash
