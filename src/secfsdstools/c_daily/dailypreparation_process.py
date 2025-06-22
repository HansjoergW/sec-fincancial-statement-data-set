"""
Module for preparing daily SEC financial statement data. Handles downloading,
transforming, and indexing daily SEC filings.
Provides functionality to process daily files starting from a specified quarter.
"""

import logging

from secdaily._00_common.BaseDefinitions import QuarterInfo
from secdaily.SecDaily import Configuration, SecDailyOrchestrator

from secfsdstools.c_automation.task_framework import AbstractProcess
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor

LOGGER = logging.getLogger(__name__)


class DailyPreparationProcess(AbstractProcess):
    """
    Process for preparing daily SEC financial statement data.

    This class handles the entire process of downloading daily SEC filings,
    transforming them into the appropriate format, and indexing them for
    efficient access. It works with the secdaily package to download and
    process the daily files.
    """

    def __init__(
        self,
        db_dir: str,
        daily_dir: str
    ):
        super().__init__()
        self.daily_dir = daily_dir
        self.index_accessor = ParquetDBIndexingAccessor(db_dir=db_dir)

    @staticmethod
    def _calculate_daily_start_quarter(quarter_before: str) -> QuarterInfo:
        year_str, quarter_str = quarter_before.split("q")
        year = int(year_str)
        quarter = int(quarter_str)

        if quarter == 4:
            year += 1
            quarter = 1
        else:
            quarter += 1

        return QuarterInfo(year, quarter)

    @staticmethod
    def _cut_off_day(quarter: QuarterInfo) -> int:
        """
        calculates the "first" day of the quarter.
        quarter one will result in yyyy0000, quarter two in yyyy0300,
        quarter three in yyyy0600, and quarter four in yyyy0900.

        This way, we can select for < cut_off_day to get all filings before the start of the quarter.
        """
        return quarter.year * 10_000 + (quarter.qrtr - 1) * 300

    def clear_index_tables(self, cut_off_day: int):
        """
        Clear index tables for the daily processing.

        index_parquet_reports: Removes entries that were created based on daily files that
        are now covered by quarterly files. Based on fields origin_file and originFileType.

        index_parquet_processing_state: TBD
        """

    def clear_daily_parquet_files(self, cut_off_day: int):
        """
        Clear daily parquet files.

        Clear parquet files that were created from daily files that
        are now covered by quarterly files.
        """

    def download_daily_files(self, daily_start_quarter: QuarterInfo):
        """
        Download daily SEC filing files.

        This method configures and uses the SecDailyOrchestrator to download
        and process daily SEC filing data, starting from the calculated
        daily_start_quarter.
        """
        config = Configuration(
            workdir=self.daily_dir,
            clean_db_entries=True,
            clean_daily_zip_files=True,
            clean_intermediate_files=True,
            clean_quarter_zip_files=True,
        )

        sec_daily = SecDailyOrchestrator(configuration=config)
        sec_daily.process_index_data(start_qrtr_info=daily_start_quarter)
        sec_daily.process_xml_data()
        sec_daily.create_sec_style()
        sec_daily.create_daily_zip()
        sec_daily.housekeeping(start_qrtr_info=daily_start_quarter)

    def process(self):
        """
        Execute the complete daily preparation process.

        This method runs the entire process in sequence:
        1. Clear index tables
        2. Clear daily parquet files
        3. Download daily files
        4. Transform daily files
        5. Index daily files
        """

        last_processed_quarter_file_name = self.index_accessor.find_latest_quarter_file_name()
        if last_processed_quarter_file_name is None:
            raise ValueError(
                "No quarterly files were processed before. "
                "Please process quarterly files first before running the daily process."
            )

        last_processed_quarter = last_processed_quarter_file_name.split(".")[0]

        LOGGER.info("starting daily processing after last processed quarter: %s", last_processed_quarter)

        daily_start_quarter = self._calculate_daily_start_quarter(last_processed_quarter)
        cut_off_day = self._cut_off_day(daily_start_quarter)

        self.clear_index_tables(cut_off_day=cut_off_day)
        self.clear_daily_parquet_files(cut_off_day=cut_off_day)
        self.download_daily_files(daily_start_quarter=daily_start_quarter)
