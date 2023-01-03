"""Database logic to hanlde the indexing"""
from dataclasses import dataclass
from typing import List

import pandas as pd

from secfsdstools._0_utils.dbutils import DB


@dataclass
class IndexReport:
    """ dataclass for index_reports table"""
    adsh: str  # pylint: disable=C0103
    cik: int
    name: str
    form: str
    filed: int
    period: int
    originFile: str  # pylint: disable=C0103
    originFileType: str  # pylint: disable=C0103


@dataclass
class IndexFileProcessingState:
    """ dataclass for index_file_processing_state table"""
    fileName: str  # pylint: disable=C0103
    fullPath: str  # pylint: disable=C0103
    status: str
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
        :return: List with IndexReport objects
        """
        sql = f'SELECT * FROM {self.INDEX_REPORTS_TABLE}'
        return self.execute_fetchall_typed(sql, IndexReport)

    def read_all_indexreports_df(self) -> pd.DataFrame:
        """
        reads all entries of the index_reports table as a pandas DataFrame
        :return: pandas DataFrame
        """
        sql = f'SELECT * FROM {self.INDEX_REPORTS_TABLE}'
        return self.execute_read_as_df(sql)

    def read_all_indexfileprocessing(self) -> List[IndexFileProcessingState]:
        """
        reads all entries of the index_file_processing_state table
        :return: List with IndexFileProcessingState objects
        """
        sql = f'SELECT * FROM {self.INDEX_PROCESSING_TABLE}'
        return self.execute_fetchall_typed(sql, IndexFileProcessingState)

    def read_all_indexfileprocessing_df(self) -> pd.DataFrame:
        """
        reads all entries of the index_file_processing_state table as a pandas DataFrame
        :return: pandas DataFrame
        """
        sql = f'SELECT * FROM {self.INDEX_PROCESSING_TABLE}'
        return self.execute_read_as_df(sql)

    def insert_indexreport(self, data: IndexReport):
        """
        inserts an entry into the index_report table
        :param data: IndexReport data object
        """
        sql = self.create_insert_statement_for_dataclass(self.INDEX_REPORTS_TABLE, data)
        self.execute_single(sql)

    def append_indexreport_df(self, dataframe: pd.DataFrame):
        """
        append the content of the df to the index report table
        :param dataframe: the dataframe to be appended
        """
        self.append_df_to_table(table_name=self.INDEX_REPORTS_TABLE, dataframe=dataframe)

    def insert_indexfileprocessing(self, data: IndexFileProcessingState):
        """
        inserts an entry into the index_file_processing_state table
        :param data: IndexFileProcessingState data object
        """
        sql = self.create_insert_statement_for_dataclass(self.INDEX_PROCESSING_TABLE, data)
        self.execute_single(sql)
