"""Indexing the downloaded to data"""
import logging
import os
from datetime import datetime, timezone
from typing import List

import pandas as pd

from secfsdstools.a_utils.constants import SUB_TXT
from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.c_automation.task_framework import AbstractProcess
from secfsdstools.c_index.indexdataaccess import IndexFileProcessingState, ParquetDBIndexingAccessor

LOGGER = logging.getLogger(__name__)


class IndexingTask:
    """ Indexes the content of a folder by writing the content of the sub_df file into the
    index tables.
    """
    PROCESSED_STR: str = 'processed'
    URL_PREFIX: str = 'https://www.sec.gov/Archives/edgar/data/'

    def __init__(self,
                 dbaccessor: ParquetDBIndexingAccessor,
                 file_path: str,
                 file_type: str,
                 process_time: str,
                 ):
        """
        Constructor.
        Args:
            dbaccessor: dbaccessor helper class
            file_path: path to the directory with the sub_df file that has to be indexed
            file_type: file_type, normally, this is "quarter"
            process_time: process time that is used a timestamp in the created table entry
        """
        self.dbaccessor = dbaccessor
        self.file_path = file_path
        self.file_type = file_type
        self.file_name = os.path.basename(file_path)
        self.process_time = process_time

    def _get_sub_df(self) -> pd.DataFrame:
        """
        reads the content of the sub_df file into dataframe.
        Returns:
            pd.DataFrame

        """
        sub_file = os.path.join(self.file_path, f"{SUB_TXT}.parquet")
        usecols = ['adsh',
                   'cik',
                   'name',
                   'form',
                   'filed',
                   'period']
        return pd.read_parquet(sub_file, columns=usecols)

    def prepare(self):
        """ prepare Task. Nothing to do."""

    def execute(self):
        """
            Reads the sub_df content and writes the entries to the index.
        """
        logger = logging.getLogger()
        logger.info("indexing file %s", self.file_name)

        # todo: check if table already contains entries
        #  will fail at the moment, since the the primary key is defined
        sub_df = self._get_sub_df()

        sub_df['fullPath'] = self.file_path
        sub_df['originFile'] = self.file_name
        sub_df['originFileType'] = self.file_type
        sub_df['url'] = self.URL_PREFIX
        sub_df['url'] = sub_df['url'] + sub_df['cik'].astype(str) + '/' + \
                        sub_df['adsh'].str.replace('-', '') + '/' + sub_df['adsh'] + '-index.htm'

        self.dbaccessor.add_index_report(sub_df,
                                         IndexFileProcessingState(
                                             fileName=self.file_name,
                                             fullPath=self.file_path,
                                             status=self.PROCESSED_STR,
                                             entries=len(sub_df),
                                             processTime=self.process_time
                                         ))

    def commit(self):
        """ no special commit handling. """
        # no special commit necessary
        return "success"

    def exception(self, exception) -> str:
        """ no special exception handling. """
        return f"failed {exception}"

    def __str__(self) -> str:
        return f"IndexingTask(file_path: {self.file_path})"


class ReportParquetIndexerProcess(AbstractProcess):
    """
    Index the reports in parquet files.
    """

    def __init__(self,
                 db_dir: str,
                 file_type: str,
                 parquet_dir: str):
        """
        Constructor.
        Args:
            db_dir: location of the dbfile.
            file_type: type of the data, usually this is "quarter".
            parquet_dir: parent directory in which the transformed parquet files are.
        """
        # only use serial execution, since indexing is rather quick
        super().__init__(execute_serial=True, chunksize=0)
        self.dbaccessor = ParquetDBIndexingAccessor(db_dir=db_dir)
        self.file_type = file_type
        self.parquet_dir = parquet_dir

        # get current datetime in UTC
        utc_dt = datetime.now(timezone.utc)
        # convert UTC time to ISO 8601 format string
        iso_date = utc_dt.astimezone().isoformat()

        self.process_time = iso_date

    def get_present_files(self) -> List[str]:
        """
        returns the available folders within the parquet directory.
        Returns:
            List[str] list with foldernames.
        """
        return get_directories_in_directory(
            os.path.join(self.parquet_dir, self.file_type))

    def _calculate_not_indexed_file_paths(self) -> List[str]:
        """
            calculates which parquet files were not indexed yet.
        Returns:
            List[str]: list with directories which need to be indexed.
        """
        present_files = self.get_present_files()
        processed_indexfiles_df = self.dbaccessor.read_all_indexfileprocessing_df()

        indexed_df = processed_indexfiles_df[
            processed_indexfiles_df.status == IndexingTask.PROCESSED_STR]
        indexed_files = indexed_df.fileName.to_list()

        not_indexed_file_names = list(set(present_files) - set(indexed_files))

        return [os.path.realpath(os.path.join(self.parquet_dir, self.file_type, file_name))
                for file_name in not_indexed_file_names]

    def calculate_tasks(self) -> List[IndexingTask]:
        """
        Calculates the tasks, which have to be executed.
        Returns:
            List[IndexingTasks]
        """
        not_indexed_paths: List[str] = self._calculate_not_indexed_file_paths()

        return [IndexingTask(dbaccessor=self.dbaccessor,
                             file_path=file_path,
                             file_type=self.file_type,
                             process_time=self.process_time)
                for file_path in not_indexed_paths]
