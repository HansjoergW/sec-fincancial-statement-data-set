from secdaily._00_common.BaseDefinitions import QuarterInfo
from secdaily.SecDaily import Configuration, SecDailyOrchestrator


class DailyPreparationProcess:
    """
    Process for preparing daily SEC financial statement data.

    This class handles the entire process of downloading daily SEC filings,
    transforming them into the appropriate format, and indexing them for
    efficient access. It works with the secdaily package to download and
    process the daily files.
    """

    def __init__(
        self,
        daily_dir: str,
        last_processed_quarter: str,  # in the form of 2022q1
        execute_serial: bool = False,
    ):
        self.daily_dir = daily_dir
        self.last_processed_quarter = last_processed_quarter
        self.execute_serial = execute_serial

        self.daily_start_quarter = self._calculate_daily_start_quarter(self.last_processed_quarter)

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

    def clear_index_tables(self):
        """
        Clear index tables for the daily processing.

        This method removes any existing index entries for the daily files
        to prepare for a fresh indexing process.
        """
        pass

    def clear_daily_parquet_files(self):
        """
        Clear daily parquet files.

        This method removes any existing daily parquet files to prepare
        for a fresh transformation process.
        """
        pass

    def download_daily_files(self):
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
        sec_daily.process_index_data(start_qrtr_info=self.daily_start_quarter)
        sec_daily.process_xml_data()
        sec_daily.create_sec_style()
        sec_daily.create_daily_zip()
        sec_daily.housekeeping(start_qrtr_info=self.daily_start_quarter)

    def transform_daily_files(self):
        """
        Transform downloaded daily files into parquet format.

        This method processes the downloaded daily files and transforms
        them into the parquet format for efficient storage and querying.
        """
        pass

    def index_daily_files(self):
        """
        Index the transformed daily files.

        This method creates indexes for the transformed daily files to
        enable efficient searching and retrieval.
        """
        pass

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
        self.clear_index_tables()
        self.clear_daily_parquet_files()
        self.download_daily_files()
        self.transform_daily_files()
        self.index_daily_files()
