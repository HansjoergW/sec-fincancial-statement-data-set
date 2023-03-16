"""Database logic to hanlde the indexing"""
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from secfsdstools.a_utils.dbutils import DB


@dataclass
class IndexReport:
    """ dataclass for index_reports table"""
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
    """ dataclass for index_file_processing_state table"""
    fileName: str  # pylint: disable=C0103
    fullPath: str  # pylint: disable=C0103
    status: str
    entries: int
    processTime: str  # pylint: disable=C0103


class DBIndexingAccessor(DB):
    """ Dataaccess class for index related tables"""
    INDEX_REPORTS_TABLE = 'index_reports'
    INDEX_PROCESSING_TABLE = 'index_file_processing_state'

    def __init__(self, db_dir: str):
        super().__init__(db_dir=db_dir)

    def read_all_indexreports(self) -> List[IndexReport]:
        """
        reads all entries of the index_reports table

        Returns:
            List[IndexReport]: List with IndexReport objects
        """
        sql = f'SELECT * FROM {self.INDEX_REPORTS_TABLE}'
        return self.execute_fetchall_typed(sql, IndexReport)

    def read_all_indexreports_df(self) -> pd.DataFrame:
        """
        reads all entries of the index_reports table as a pandas DataFrame

        Returns:
            pd.DataFrame: pandas DataFrame
        """
        sql = f'SELECT * FROM {self.INDEX_REPORTS_TABLE}'
        return self.execute_read_as_df(sql)

    def read_all_indexfileprocessing(self) -> List[IndexFileProcessingState]:
        """
        reads all entries of the index_file_processing_state table

        Returns:
            List[IndexFileProcessingState]: List with IndexFileProcessingState objects
        """
        sql = f'SELECT * FROM {self.INDEX_PROCESSING_TABLE}'
        return self.execute_fetchall_typed(sql, IndexFileProcessingState)

    def read_all_indexfileprocessing_df(self) -> pd.DataFrame:
        """
        reads all entries of the index_file_processing_state table as a pandas DataFrame

        Returns:
            pd.DataFrame: pandas DataFrame
        """
        sql = f'SELECT * FROM {self.INDEX_PROCESSING_TABLE}'
        return self.execute_read_as_df(sql)

    def read_index_file_for_filename(self, filename: str) -> IndexFileProcessingState:
        """
        returns the IndexFileProcessingState instance for the provided filename
        Args:
            filename (str): the filename of the file

        Returns:
            IndexFileProcessingState: the processing state instance
        """
        sql = f"SELECT * FROM {self.INDEX_PROCESSING_TABLE} WHERE fileName = '{filename}'"
        return self.execute_fetchall_typed(sql, IndexFileProcessingState)[0]

    def insert_indexreport(self, data: IndexReport):
        """
        inserts an entry into the index_report table
        Args:
            data (IndexReport): IndexReport data object
        """
        sql = self.create_insert_statement_for_dataclass(self.INDEX_REPORTS_TABLE, data)
        self.execute_single(sql)

    def append_indexreport_df(self, dataframe: pd.DataFrame):
        """
        append the content of the df to the index report table

        Args:
            dataframe (pd.DataFrame): the dataframe to be appended
        """
        self.append_df_to_table(table_name=self.INDEX_REPORTS_TABLE, dataframe=dataframe)

    def insert_indexfileprocessing(self, data: IndexFileProcessingState):
        """
        inserts an entry into the index_file_processing_state table

        Args:
            data (IndexFileProcessingState): IndexFileProcessingState data object to insert
        """
        sql = self.create_insert_statement_for_dataclass(self.INDEX_PROCESSING_TABLE, data)
        self.execute_single(sql)

    def find_latest_company_report(self, cik: int) -> IndexReport:
        """
        returns the latest report of a company

        Args:
            cik (int): the cik of the company

        Returns:
            IndexReport: of the latest report of the company
        """

        sql = f"""SELECT *
                    FROM {self.INDEX_REPORTS_TABLE}
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
                    FROM {self.INDEX_REPORTS_TABLE}
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
                    FROM {self.INDEX_REPORTS_TABLE}
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

    def read_index_reports_for_cik(self, cik: int, forms: Optional[List[str]] = None) \
            -> List[IndexReport]:
        """
        gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned

        Args:
            cik (int): cik of the company
            forms (List[str], optional, None): list of the forms to be returend,
             like ['10-Q', '10-K']
        Returns:
            List[IndexReport]
        """
        sql = f'SELECT * FROM {self.INDEX_REPORTS_TABLE} WHERE cik = {cik}'
        if forms is not None:
            forms_str = ", ".join(["'" + x.upper() + "'" for x in forms])
            sql = sql + f' and form in ({forms_str}) '
        sql = sql + ' ORDER BY period DESC'

        return self.execute_fetchall_typed(sql, IndexReport)

    def read_index_reports_for_cik_df(self, cik: int, forms: Optional[List[str]] = None) \
            -> pd.DataFrame:
        """
        gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned

        Args:
            cik (int): cik of the company
            forms (List[str], optional, None):  list of the forms to be returend,
             like ['10-Q', '10-K']
        Returns:
            pd.DataFrame
        """
        sql = f'SELECT * FROM {self.INDEX_REPORTS_TABLE} WHERE cik = {cik}'
        if forms is not None:
            forms_str = ", ".join(["'" + x.upper() + "'" for x in forms])
            sql = sql + f' and form in ({forms_str}) '
        sql = sql + ' ORDER BY period DESC'

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
                SELECT DISTINCT name, cik from {self.INDEX_REPORTS_TABLE} 
                WHERE name like '%{name_part}%' 
                ORDER BY name"""
        return self.execute_read_as_df(sql)
