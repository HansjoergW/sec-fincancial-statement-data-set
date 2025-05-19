"""Database logic to hanlde the indexing"""

import sqlite3
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from secfsdstools.a_utils.dbutils import DB


@dataclass
class IndexReport:
    """dataclass for index_reports table"""

    adsh: str
    cik: int
    name: str
    form: str
    filed: int
    period: int
    fullPath: str  # pylint: disable=C0103
    originFile: str  # pylint: disable=C0103
    originFileType: str  # pylint: disable=C0103
    url: str


@dataclass
class IndexFileProcessingState:
    """dataclass for index_file_processing_state table"""

    fileName: str  # pylint: disable=C0103
    fullPath: str  # pylint: disable=C0103
    status: str
    entries: int
    processTime: str  # pylint: disable=C0103


class ParquetDBIndexingAccessor(DB):
    """Dataaccess class for index related tables of parquet files"""

    index_reports_table = "index_parquet_reports"
    index_processing_table = "index_parquet_processing_state"

    def __init__(self, db_dir: str):
        super().__init__(db_dir=db_dir)

    def read_all_indexreports(self) -> List[IndexReport]:
        """
        reads all entries of the index_reports table

        Returns:
            List[IndexReport]: List with IndexReport objects
        """
        sql = f"SELECT * FROM {self.index_reports_table}"
        return self.execute_fetchall_typed(sql, IndexReport)

    def read_all_indexreports_df(self) -> pd.DataFrame:
        """
        reads all entries of the index_reports table as a pandas DataFrame

        Returns:
            pd.DataFrame: pandas DataFrame
        """
        sql = f"SELECT * FROM {self.index_reports_table}"
        return self.execute_read_as_df(sql)

    def find_latest_quarter_file_name(self) -> Optional[str]:
        """
        finds the latest quarter in the index_reports table

        Returns:
            str: the latest quarter
        """
        sql = f"""
                SELECT *
                FROM {self.index_processing_table}
                WHERE fileName LIKE '____q_.zip' AND status = 'processed'
                ORDER BY fileName DESC
                LIMIT 1
        """
        result = self.execute_fetchall_typed(sql, IndexFileProcessingState)
        if not result:
            return None

        return result[0].fileName

    def read_all_indexfileprocessing(self) -> List[IndexFileProcessingState]:
        """
        reads all entries of the index_file_processing_state table

        Returns:
            List[IndexFileProcessingState]: List with IndexFileProcessingState objects
        """
        sql = f"SELECT * FROM {self.index_processing_table}"
        return self.execute_fetchall_typed(sql, IndexFileProcessingState)

    def read_all_indexfileprocessing_df(self) -> pd.DataFrame:
        """
        reads all entries of the index_file_processing_state table as a pandas DataFrame

        Returns:
            pd.DataFrame: pandas DataFrame
        """
        sql = f"SELECT * FROM {self.index_processing_table}"
        return self.execute_read_as_df(sql)

    def read_index_file_for_filename(self, filename: str) -> IndexFileProcessingState:
        """
        returns the IndexFileProcessingState instance for the provided filename
        Args:
            filename (str): the filename of the file

        Returns:
            IndexFileProcessingState: the processing state instance
        """
        sql = f"SELECT * FROM {self.index_processing_table} WHERE fileName = '{filename}'"
        return self.execute_fetchall_typed(sql, IndexFileProcessingState)[0]

    def read_index_files_for_filenames(self, filenames: List[str]) -> List[IndexFileProcessingState]:
        """
        returns the IndexFileProcessingState instances for the provided filenames
        Args:
            filenames (List[str]): the filenames of the files

        Returns:
            List[IndexFileProcessingState]: the processing state instance
        """
        filenames_str = ", ".join(["'" + x + "'" for x in filenames])

        sql = f"SELECT * FROM {self.index_processing_table} WHERE fileName in ({filenames_str})"
        return self.execute_fetchall_typed(sql, IndexFileProcessingState)

    def insert_indexreport(self, data: IndexReport):
        """
        inserts an entry into the index_report table
        Args:
            data (IndexReport): IndexReport data object
        """
        sql = self.create_insert_statement_for_dataclass(self.index_reports_table, data)
        with self.get_connection() as conn:
            self.execute_single(sql, conn)

    def add_index_report(self, sub_df: pd.DataFrame, processing_state: IndexFileProcessingState):
        """
        adds the submissions in the sub_df into the index table and stores the processing state
        in the processing table
        Args:
            sub_df: dataframe with submissions
            processing_state: state entry to write

        Returns:

        """

        with self.get_connection() as conn:
            self._append_indexreport_df(sub_df, conn)
            self._insert_indexfileprocessing(processing_state, conn)

    def _append_indexreport_df(self, dataframe: pd.DataFrame, conn: sqlite3.Connection):
        """
        append the content of the df to the index report table

        Args:
            dataframe (pd.DataFrame): the dataframe to be appended
        """
        self.append_df_to_table(table_name=self.index_reports_table, dataframe=dataframe, conn=conn)

    def insert_indexfileprocessing(self, data: IndexFileProcessingState):
        """
        inserts an entry into the index_file_processing_state table

        Args:
            data (IndexFileProcessingState): IndexFileProcessingState data object to insert
        """
        with self.get_connection() as conn:
            self._insert_indexfileprocessing(data, conn)

    def _insert_indexfileprocessing(self, data: IndexFileProcessingState, conn: sqlite3.Connection):
        """
        inserts an entry into the index_file_processing_state table

        Args:
            data (IndexFileProcessingState): IndexFileProcessingState data object to insert
        """
        sql = self.create_insert_statement_for_dataclass(self.index_processing_table, data)
        self.execute_single(sql, conn)

    def find_latest_company_report(self, cik: int) -> IndexReport:
        """
        returns the latest report of a company

        Args:
            cik (int): the cik of the company

        Returns:
            IndexReport: of the latest report of the company
        """

        sql = f"""SELECT *
                    FROM {self.index_reports_table}
                    WHERE cik = {cik} and originFileType = 'quarter'
                    ORDER BY period DESC"""
        return self.execute_fetchall_typed(sql, IndexReport)[0]

    def read_index_report_for_adsh(self, adsh: str) -> IndexReport:
        """
        returns the IndexReport instance for the provided adsh

        Args:
            adsh (str):  adsh
        Returns:
            IndexReport: the report for the provided adsh
        """
        # sorting by originfiletype, so we prefer official data from SEC,
        # over the daily files, in case both should be present.
        sql = f"""SELECT *
                    FROM {self.index_reports_table}
                    WHERE adsh = '{adsh}'
                    ORDER BY originFileType DESC"""
        return self.execute_fetchall_typed(sql, IndexReport)[0]

    def read_index_reports_for_adshs(self, adshs: List[str]) -> List[IndexReport]:
        """
        returns the IndexReport instances for the provided adshs

        Args:
            adshs (List[str]):  adshs
        Returns:
            List[IndexReport]: the reports for the provided adshs
        """

        adshs_str = ", ".join(["'" + x.upper() + "'" for x in adshs])
        # sorting by originfiletype, so we prefer official data from SEC,
        # over the daily files, in case both should be present.
        sql = f"""SELECT *
                    FROM {self.index_reports_table}
                    WHERE adsh in ({adshs_str})
                    ORDER BY adsh, originFileType DESC"""

        reports: List[IndexReport] = self.execute_fetchall_typed(sql, IndexReport)

        last_adsh = None
        filtered_reports: List[IndexReport] = []
        for report in reports:
            if last_adsh == report.adsh:
                continue
            last_adsh = report.adsh
            filtered_reports.append(report)

        return filtered_reports

    def read_index_reports_for_ciks(self, ciks: List[int], forms: Optional[List[str]] = None) -> List[IndexReport]:
        """
        gets all reports as IndexReport instances for the companies identified by their cik numbers.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned

        Args:
            ciks (List[int]): ciks of the companies
            forms (List[str], optional, None): list of the forms to be returend,
             like ['10-Q', '10-K']
        Returns:
            List[IndexReport]
        """
        ciks_str = [str(cik) for cik in ciks]
        sql = f'SELECT * FROM {self.index_reports_table} WHERE cik in ({",".join(ciks_str)})'
        if forms is not None:
            forms_str = ", ".join(["'" + x.upper() + "'" for x in forms])
            sql = sql + f" and form in ({forms_str}) "
        sql = sql + " ORDER BY period DESC"

        return self.execute_fetchall_typed(sql, IndexReport)

    def read_index_reports_for_ciks_df(self, ciks: List[int], forms: Optional[List[str]] = None) -> pd.DataFrame:
        """
        gets all reports as IndexReport instances for the companies identified by their cik numbers.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned

        Args:
            ciks (List[int]): ciks of the companies
            forms (List[str], optional, None):  list of the forms to be returend,
             like ['10-Q', '10-K']
        Returns:
            pd.DataFrame
        """
        ciks_str = [str(cik) for cik in ciks]
        sql = f'SELECT * FROM {self.index_reports_table} WHERE cik in ({",".join(ciks_str)})'
        if forms is not None:
            forms_str = ", ".join(["'" + x.upper() + "'" for x in forms])
            sql = sql + f" and form in ({forms_str}) "
        sql = sql + " ORDER BY period DESC"

        return self.execute_read_as_df(sql)

    def find_company_by_name(self, name_part: str) -> pd.DataFrame:
        """
        Finds companies in the index based on the provided part of the name.
        Lower and uppercase are ignored.

        Args:
            name_part: the part of the name

        Returns:
            pd.DataFrame: with columns name and cik
        """
        sql = f"""
                SELECT DISTINCT name, cik from {self.index_reports_table}
                WHERE name like '%{name_part}%'
                ORDER BY name"""
        return self.execute_read_as_df(sql)

    # pylint: disable=C0103
    def read_filenames_by_type(self, originFileType: str = "quarter") -> List[str]:
        """
        Returns all filenames of the provided file type (usually "quarter")
        Args:
            origin_file_type:

        Returns:
            List[str]: filenames of the type

        """
        sql = f"""
                 SELECT ORIGINFILE
                 FROM {self.index_reports_table}
                 WHERE ORIGINFILETYPE='{originFileType}'"""

        # result  is a list of tuples with one entry, so we have to flatten it
        result = self.execute_fetchall(sql=sql)
        return [x[0] for x in result]
