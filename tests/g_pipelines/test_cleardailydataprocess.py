"""
Tests for the ClearDailyDataProcess class.

Tests cover the process method, clear_directory method, and integration
with DailyPreparationProcess static methods for quarter calculations.
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from secdaily._00_common.BaseDefinitions import QuarterInfo

from secfsdstools.x_examples.automation.memory_optimized_daily_automation import ClearDailyDataProcess


@pytest.fixture
def mock_index_accessor():
    """Create a mock ParquetDBIndexingAccessor for testing."""
    return Mock()


@pytest.fixture
def clear_process(mock_index_accessor):
    """Create a ClearDailyDataProcess instance with mocked dependencies."""
    with patch(
        "secfsdstools.x_examples.automation.memory_optimized_daily_automation.ParquetDBIndexingAccessor"
    ) as mock_accessor_class:
        mock_accessor_class.return_value = mock_index_accessor

        return ClearDailyDataProcess(
            db_dir="/test/db",
            filtered_daily_joined_by_stmt_dir="/test/filtered",
            standardized_daily_by_stmt_dir="/test/standardized",
        )


def test_init():
    """Test ClearDailyDataProcess initialization."""
    with patch(
        "secfsdstools.x_examples.automation.memory_optimized_daily_automation.ParquetDBIndexingAccessor"
    ) as mock_accessor_class:
        mock_accessor = Mock()
        mock_accessor_class.return_value = mock_accessor

        process = ClearDailyDataProcess(
            db_dir="/test/db",
            filtered_daily_joined_by_stmt_dir="/test/filtered",
            standardized_daily_by_stmt_dir="/test/standardized",
        )

        assert process.db_dir == "/test/db"
        assert process.filtered_daily_joined_by_stmt_dir == "/test/filtered"
        assert process.standardized_daily_by_stmt_dir == "/test/standardized"
        assert process.index_accessor == mock_accessor
        mock_accessor_class.assert_called_once_with(db_dir="/test/db")


def test_clear_directory_with_directories_to_remove(tmp_path):
    """Test clear_directory removes directories older than cut_off_day."""
    # Setup
    root_dir = tmp_path / "test_root"
    root_dir.mkdir()

    # Create directories that should be removed (before cut_off)
    (root_dir / "20220101.zip").mkdir()
    (root_dir / "20220201.zip").mkdir()
    (root_dir / "20220301.zip").mkdir()

    # Create directories that should remain (after cut_off)
    (root_dir / "20220401.zip").mkdir()
    (root_dir / "20220501.zip").mkdir()

    # Create a file (should be ignored)
    (root_dir / "somefile.txt").touch()

    with patch("secfsdstools.x_examples.automation.memory_optimized_daily_automation.ParquetDBIndexingAccessor"):
        process = ClearDailyDataProcess(
            db_dir="/test/db",
            filtered_daily_joined_by_stmt_dir="/test/filtered",
            standardized_daily_by_stmt_dir="/test/standardized",
        )

        # Execution
        cut_off_day = 20220400  # April 1st, 2022
        process.clear_directory(cut_off_day=cut_off_day, root_dir_path=root_dir)

        # Assertion
        remaining_items = [item.name for item in root_dir.iterdir()]

        # Directories before cut_off should be removed
        assert "20220101.zip" not in remaining_items
        assert "20220201.zip" not in remaining_items
        assert "20220301.zip" not in remaining_items

        # Directories after cut_off should remain
        assert "20220401.zip" in remaining_items
        assert "20220501.zip" in remaining_items

        # Files should be ignored and remain
        assert "somefile.txt" in remaining_items


def test_clear_directory_with_no_directories_to_remove(tmp_path):
    """Test clear_directory when no directories need to be removed."""
    # Setup
    root_dir = tmp_path / "test_root"
    root_dir.mkdir()

    # Create directories that should all remain (after cut_off)
    (root_dir / "20220501.zip").mkdir()
    (root_dir / "20220601.zip").mkdir()

    with patch("secfsdstools.x_examples.automation.memory_optimized_daily_automation.ParquetDBIndexingAccessor"):
        process = ClearDailyDataProcess(
            db_dir="/test/db",
            filtered_daily_joined_by_stmt_dir="/test/filtered",
            standardized_daily_by_stmt_dir="/test/standardized",
        )

        # Execution
        cut_off_day = 20220400  # April 1st, 2022
        process.clear_directory(cut_off_day=cut_off_day, root_dir_path=root_dir)

        # Assertion
        remaining_items = [item.name for item in root_dir.iterdir()]

        # All directories should remain
        assert "20220501.zip" in remaining_items
        assert "20220601.zip" in remaining_items
        assert len(remaining_items) == 2


def test_clear_directory_nonexistent_directory():
    """Test clear_directory when root directory does not exist."""
    with patch("secfsdstools.x_examples.automation.memory_optimized_daily_automation.ParquetDBIndexingAccessor"):
        process = ClearDailyDataProcess(
            db_dir="/test/db",
            filtered_daily_joined_by_stmt_dir="/test/filtered",
            standardized_daily_by_stmt_dir="/test/standardized",
        )

        # Execution - should not raise an error
        nonexistent_path = Path("/nonexistent/path")
        cut_off_day = 20220400
        process.clear_directory(cut_off_day=cut_off_day, root_dir_path=nonexistent_path)

        # Assertion - method should complete without error
        # No specific assertions needed as we're testing it doesn't crash


def test_clear_directory_empty_directory(tmp_path):
    """Test clear_directory with an empty directory."""
    # Setup
    root_dir = tmp_path / "empty_root"
    root_dir.mkdir()

    with patch("secfsdstools.x_examples.automation.memory_optimized_daily_automation.ParquetDBIndexingAccessor"):
        process = ClearDailyDataProcess(
            db_dir="/test/db",
            filtered_daily_joined_by_stmt_dir="/test/filtered",
            standardized_daily_by_stmt_dir="/test/standardized",
        )

        # Execution
        cut_off_day = 20220400
        process.clear_directory(cut_off_day=cut_off_day, root_dir_path=root_dir)

        # Assertion
        assert root_dir.exists()
        assert len(list(root_dir.iterdir())) == 0


def test_process_successful_execution(clear_process, mock_index_accessor):
    """Test successful execution of the process method."""
    # Setup
    mock_index_accessor.find_latest_quarter_file_name.return_value = "2022q1.zip"

    with patch.object(clear_process, "clear_directory") as mock_clear_directory:
        # Execution
        clear_process.process()

        # Assertions
        mock_index_accessor.find_latest_quarter_file_name.assert_called_once()

        # Verify clear_directory was called twice with correct parameters
        assert mock_clear_directory.call_count == 2

        # Check first call (filtered daily data)
        first_call = mock_clear_directory.call_args_list[0]
        assert first_call[1]["cut_off_day"] == 20220400  # Q2 2022 cut-off
        assert first_call[1]["root_dir_path"] == Path("/test/filtered/daily")

        # Check second call (standardized daily data)
        second_call = mock_clear_directory.call_args_list[1]
        assert second_call[1]["cut_off_day"] == 20220400  # Q2 2022 cut-off
        assert second_call[1]["root_dir_path"] == Path("/test/standardized")


def test_process_no_quarterly_files_processed(clear_process, mock_index_accessor):
    """Test process method when no quarterly files have been processed."""
    # Setup
    mock_index_accessor.find_latest_quarter_file_name.return_value = None

    # Execution & Assertion
    with pytest.raises(ValueError, match="No quarterly files were processed before"):
        clear_process.process()


def test_process_with_different_quarters(clear_process, mock_index_accessor):
    """Test process method with different quarter transitions."""
    test_cases = [
        ("2022q1.zip", 20220400),  # Q1 -> Q2, cut-off April
        ("2022q2.zip", 20220700),  # Q2 -> Q3, cut-off July
        ("2022q3.zip", 20221000),  # Q3 -> Q4, cut-off October
        ("2022q4.zip", 20230000),  # Q4 -> Q1 next year, cut-off January
    ]

    for quarter_file, expected_cut_off in test_cases:
        # Setup
        mock_index_accessor.find_latest_quarter_file_name.return_value = quarter_file

        with patch.object(clear_process, "clear_directory") as mock_clear_directory:
            # Execution
            clear_process.process()

            # Assertion
            # Both calls should use the same cut_off_day
            for call in mock_clear_directory.call_args_list:
                assert call[1]["cut_off_day"] == expected_cut_off


def test_process_integration_with_daily_preparation_methods(clear_process, mock_index_accessor):
    """Test that process method correctly integrates with DailyPreparationProcess static methods."""
    # Setup
    mock_index_accessor.find_latest_quarter_file_name.return_value = "2023q3.zip"

    with patch.object(clear_process, "clear_directory") as mock_clear_directory:
        # Execution
        clear_process.process()

        # Assertions
        # Verify the quarter calculation logic
        # 2023q3 -> 2023q4, cut_off should be 20231000 (October)
        expected_cut_off = 20231000

        for call in mock_clear_directory.call_args_list:
            assert call[1]["cut_off_day"] == expected_cut_off
