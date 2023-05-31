"""Indexing the downloaded to data"""
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Tuple

import pandas as pd

from secfsdstools.a_utils.constants import SUB_TXT
from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.c_index.indexdataaccess import IndexFileProcessingState, \
    DBIndexingAccessorBase, ParquetDBIndexingAccessor

LOGGER = logging.getLogger(__name__)


class BaseReportIndexer(ABC):
    """
    Base class to index the reports.
    """
    PROCESSED_STR: str = 'processed'
    URL_PREFIX: str = 'https://www.sec.gov/Archives/edgar/data/'

    def __init__(self, accessor: DBIndexingAccessorBase, file_type: str):
        self.dbaccessor = accessor
        self.file_type = file_type

        # get current datetime in UTC
        utc_dt = datetime.now(timezone.utc)
        # convert UTC time to ISO 8601 format
        iso_date = utc_dt.astimezone().isoformat()

        self.process_time = iso_date

    @abstractmethod
    def get_present_files(self) -> List[str]:
        """
        returns the list with the filenames that are already present
        Returns:
            List[str]: list with the zip filenames that are already present
        """

    @abstractmethod
    def get_sub_df(self, file_name: str) -> Tuple[pd.DataFrame, str]:
        """
        loads the content of sub_txt into a dataframe and returns the dataframe and the
        fullpath to the data as a tuple.
        Args:
            file_name: name of the original zip file

        Returns:
            Tuple[pd.Dataframe, str]: DataFrame with the content in the sub_txt file, file path

        """

    def _calculate_not_indexed(self) -> List[str]:
        present_files = self.get_present_files()
        processed_indexfiles_df = self.dbaccessor.read_all_indexfileprocessing_df()

        indexed_df = processed_indexfiles_df[processed_indexfiles_df.status == self.PROCESSED_STR]
        indexed_files = indexed_df.fileName.to_list()

        not_indexed = set(present_files) - set(indexed_files)
        return list(not_indexed)

    def _index_file(self, file_name: str):
        LOGGER.info("indexing file %s", file_name)

        # todo: check if table already contains entries
        #  will fail at the moment, since the the primary key is defined
        sub_df, full_path = self.get_sub_df(file_name)

        sub_df['fullPath'] = full_path
        sub_df['originFile'] = file_name
        sub_df['originFileType'] = self.file_type
        sub_df['url'] = BaseReportIndexer.URL_PREFIX
        sub_df['url'] = sub_df['url'] + sub_df['cik'].astype(str) + '/' + \
                        sub_df['adsh'].str.replace('-', '') + '/' + sub_df['adsh'] + '-index.htm'

        self.dbaccessor.add_index_report(sub_df,
                                         IndexFileProcessingState(
                                             fileName=file_name,
                                             fullPath=full_path,
                                             status=self.PROCESSED_STR,
                                             entries=len(sub_df),
                                             processTime=self.process_time
                                         ))

    def process(self):
        """
        index all not zip-files that were not indexed yet.
        """
        not_indexed_files = self._calculate_not_indexed()
        for not_indexed_file in not_indexed_files:
            self._index_file(file_name=not_indexed_file)


class ReportParquetIndexer(BaseReportIndexer):
    """
    Index the reports in parquet files.
    """

    def __init__(self, db_dir: str, parquet_dir: str, file_type: str):
        super().__init__(ParquetDBIndexingAccessor(db_dir=db_dir), file_type)
        self.parquet_dir = parquet_dir

    def get_present_files(self) -> List[str]:
        return get_directories_in_directory(
            os.path.join(self.parquet_dir, self.file_type))

    def get_sub_df(self, file_name: str) -> Tuple[pd.DataFrame, str]:
        path = os.path.join(self.parquet_dir, self.file_type, file_name)
        full_path = os.path.realpath(path)
        sub_file = os.path.join(full_path, f"{SUB_TXT}.parquet")
        usecols = ['adsh',
                   'cik',
                   'name',
                   'form',
                   'filed',
                   'period']
        return pd.read_parquet(sub_file, columns=usecols), full_path
