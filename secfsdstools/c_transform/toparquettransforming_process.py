"""Transform zip files to parquet format"""

import contextlib
import glob
import logging
import os
import shutil
from pathlib import Path
from typing import List

from secfsdstools.a_utils.constants import SUB_TXT, PRE_TXT, NUM_TXT, NUM_DTYPE, PRE_DTYPE, \
    SUB_DTYPE
from secfsdstools.a_utils.fileutils import get_directories_in_directory, \
    read_df_from_file_in_zip
from secfsdstools.c_automation.task_framework import AbstractProcess

LOGGER = logging.getLogger(__name__)


class ToParquetTransformTask:
    """
    Transforms a zip file containing csv files to a folder with parquet files.
    """

    def __init__(self,
                 zip_file_path: str,
                 parquet_dir: str,
                 file_type: str,
                 keep_zip_files: bool):
        """
        Constructor.
        Args:
            zip_file_path: path to the zipfile that must be transformed
            parquet_dir: target base directory for the parguet files
            file_type: file_type, either 'quarter' or 'daily' used to define the
                       subfolder in the parquet dir
            keep_zip_files: flag that indicates whether the zipfiles should be deleted after
                            successful transformation
        """
        self.zip_file_name = os.path.basename(zip_file_path)
        self.zip_file_path = zip_file_path
        self.parquet_dir = parquet_dir
        self.file_type = file_type
        self.keep_zip_files = keep_zip_files

        self.file_path = Path(self.parquet_dir) / self.file_type / self.zip_file_name

    def prepare(self):
        """ create the necessary parent directories. """
        self.file_path.mkdir(parents=True, exist_ok=True)

    def execute(self):
        """
        transform the zip file.
        """
        self._inner_transform_zip_file(self.file_path, self.zip_file_path)

        # remove the file if keep_zip_files is False
        if not self.keep_zip_files:
            with contextlib.suppress(OSError):
                os.remove(self.zip_file_path)

    def commit(self) -> str:
        """ nothing special to do. """
        return "success"

    def exception(self, exception) -> str:
        """ log the problem and clean the target directory. """
        logger = logging.getLogger()
        logger.error('failed to process %s', self.zip_file_name)
        # the created dir has to be removed with all its content
        shutil.rmtree(self.file_path, ignore_errors=True)
        return f"failed {exception}"

    def __str__(self) -> str:
        return f"ToParquetTransformTask(zip_file_name: {self.zip_file_name})"

    def _inner_transform_zip_file(self, target_path: Path, zip_file_path):
        sub_df = read_df_from_file_in_zip(zip_file=zip_file_path, file_to_extract=SUB_TXT,
                                          dtype=SUB_DTYPE)
        pre_df = read_df_from_file_in_zip(zip_file=zip_file_path, file_to_extract=PRE_TXT,
                                          dtype=PRE_DTYPE)

        num_df = read_df_from_file_in_zip(zip_file=zip_file_path, file_to_extract=NUM_TXT,
                                          dtype=NUM_DTYPE)

        # ensure period columns are valid ints
        # some report types don't have a value set for period
        sub_df['period'] = sub_df['period'].fillna(-1).astype(int)
        # same for line
        pre_df['line'] = pre_df['line'].fillna(-1).astype(int)

        # special handling for field value in num, since the daily files can also contain strings
        if self.file_type == 'daily':
            num_df = num_df[~num_df.tag.isin(['SecurityExchangeName', 'TradingSymbol'])]

        num_df['value'] = num_df['value'].astype(float)

        sub_df.to_parquet(target_path / f'{SUB_TXT}.parquet')
        pre_df.to_parquet(target_path / f'{PRE_TXT}.parquet')
        num_df.to_parquet(target_path / f'{NUM_TXT}.parquet')


class ToParquetTransformerProcess(AbstractProcess):
    """
    Transforming zip files containing the sub.txt, num.txt, and pre.txt as CSV into
    parquet format.
    """

    def __init__(self,
                 zip_dir: str,
                 parquet_dir: str,
                 file_type: str,
                 keep_zip_files: bool):
        """
        Constructor.
        Args:
            zip_dir: directory which contains the zipfiles that have to be transformed to parquet
            parquet_dir: target base directory for the parguet files
            file_type: file_type, either 'quarter' or 'daily' used to define the
                       subfolder in the parquet dir
        """
        super().__init__(execute_serial=False,
                         chunksize=0)

        self.zip_dir = zip_dir
        self.parquet_dir = parquet_dir
        self.file_type = file_type
        self.keep_zip_files = keep_zip_files

    def _calculate_not_transformed(self) -> List[str]:
        """
        calculates the untransformed zip files in the zip_dir.
        simply reads all the existing file names in the zip_dir and checks if there is a
         subfolder with the same name in the parguet-dir
        Returns:
            List[str]: List with tuple of zipfile path.
        """
        downloaded_zipfiles = glob.glob(os.path.join(self.zip_dir, "*.zip"))
        present_parquet_dirs = get_directories_in_directory(
            os.path.join(self.parquet_dir, self.file_type))

        zip_file_names = {os.path.basename(p): p for p in downloaded_zipfiles}
        parquet_dirs = [os.path.basename(p) for p in present_parquet_dirs]

        not_transformed_names = set(zip_file_names.keys()) - set(parquet_dirs)

        # key is the zipfile name, value is the whole path of the file
        # the returned dict only contains elements for which not parquet directory does exist yet
        return [v for k, v in zip_file_names.items() if k in not_transformed_names]

    def calculate_tasks(self) -> List[ToParquetTransformTask]:
        """

        Returns:
            List[ToParquetTransformTask]: calculates the necessary tasks that have to be executed.
        """
        not_transformed_paths: List[str] = self._calculate_not_transformed()

        return [ToParquetTransformTask(zip_file_path=zip_file_path,
                                       parquet_dir=self.parquet_dir,
                                       file_type=self.file_type,
                                       keep_zip_files=self.keep_zip_files
                                       )
                for zip_file_path in not_transformed_paths]
