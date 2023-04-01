"""Transform zip files to parquet format"""
import glob
import logging
import os
from typing import List, Tuple
import shutil

from secfsdstools.a_utils.constants import SUB_TXT, PRE_TXT, NUM_TXT, NUM_DTYPE, PRE_DTYPE, SUB_DTYPE
from secfsdstools.a_utils.fileutils import get_directories_in_directory, \
    read_df_from_file_in_zip
from secfsdstools.a_utils.parallelexecution import ParallelExecutor

LOGGER = logging.getLogger(__name__)

todo logging unit tests

class ToParquetTransformer:


    def __init__(self, zip_dir: str, parquet_dir: str, file_type: str):
        self.zip_dir = zip_dir
        self.parquet_dir = parquet_dir
        self.file_type = file_type

    def _calculate_not_transformed(self) -> List[Tuple[str, str]]:
        downloaded_zipfiles = glob.glob(os.path.join(self.zip_dir, "*.zip"))
        present_parquet_dirs = get_directories_in_directory(os.path.join(self.parquet_dir, self.file_type))

        zip_file_names = {os.path.basename(p): p for p in downloaded_zipfiles}
        parquet_dirs = [os.path.basename(p) for p in present_parquet_dirs]

        not_transformed_names = set(zip_file_names.keys()) - set(parquet_dirs)

        # key is the zipfile name, value is the whole path of the file
        # the returned dict only contains elements for which not parquet directory does exist yet
        return [(k, v) for k, v in zip_file_names.items() if k in not_transformed_names]

    def _transform_zip_file(self, zip_file_name: str, zip_file_path: str):
        # create dir
        target_path = os.path.join(self.parquet_dir, self.file_type, zip_file_name)
        try:
            os.makedirs(target_path, exist_ok=True)
            print(zip_file_name)
            sub_df = read_df_from_file_in_zip(zip_file=zip_file_path, file_to_extract=SUB_TXT, dtype=SUB_DTYPE)
            pre_df = read_df_from_file_in_zip(zip_file=zip_file_path, file_to_extract=PRE_TXT, dtype=PRE_DTYPE)
            num_df = read_df_from_file_in_zip(zip_file=zip_file_path, file_to_extract=NUM_TXT, dtype=NUM_DTYPE)

            sub_df.to_parquet(os.path.join(target_path, f'{SUB_TXT}.parquet'))
            pre_df.to_parquet(os.path.join(target_path, f'{PRE_TXT}.parquet'))
            num_df.to_parquet(os.path.join(target_path, f'{NUM_TXT}.parquet'))
        except Exception as ex:
            shutil.rmtree(target_path, ignore_errors=True)
            print(ex)

    def process(self):
        def get_entries() -> List[Tuple[str, str]]:
            return self._calculate_not_transformed()

        def process_element(element: Tuple[str, str]) -> Tuple[str, str]:
            self._transform_zip_file(element[0], element[1])
            return element

        def post_process(parts: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
            return parts

        executor = ParallelExecutor(chunksize=0)

        executor.set_get_entries_function(get_entries)
        executor.set_process_element_function(process_element)
        executor.set_post_process_chunk_function(post_process)

        # we ignore the missing, since get_entries always returns the whole list
        result, failed = executor.execute()
        print(failed)

        return result
