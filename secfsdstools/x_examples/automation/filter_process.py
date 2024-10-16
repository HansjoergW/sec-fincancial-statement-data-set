import os
import shutil
from pathlib import Path
from typing import List

from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.c_automation.task_framework import AbstractProcess, Task
from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.joinedfiltering import StmtJoinedFilter
from secfsdstools.e_filter.rawfiltering import StmtRawFilter


def postloadfilter(databag: RawDataBag) -> RawDataBag:
    """
    defines a post filter method that can be used by ZipCollectors.
    It combines the filters:
            ReportPeriodRawFilter, MainCoregRawFilter, USDOnlyRawFilter
    """
    # pylint: disable=C0415
    from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, \
        USDOnlyRawFilter

    return databag[ReportPeriodRawFilter()][MainCoregRawFilter()][USDOnlyRawFilter()]


class FilterTask:

    def __init__(self,
                 filtered_path: Path,
                 stmts: List[str]):
        self.filtered_path = filtered_path
        self.stmts = stmts

        self.file_name = filtered_path.name
        self.tmp_path = filtered_path.parent / f"tmp_{self.file_name}"

    def prepare(self):
        """ prepare Task. Nothing to do."""

        for stmt in self.stmts:
            (self.tmp_path / "raw" / stmt).mkdir(parents=True, exist_ok=False)
            (self.tmp_path / "joined" / stmt).mkdir(parents=True, exist_ok=False)

    def execute(self):
        raw_bag = ZipCollector.get_zip_by_name(name=self.file_name,
                                               forms_filter=['10-K', '10-Q'],
                                               stmt_filter=self.stmts,
                                               post_load_filter=postloadfilter).collect()

        joined_bag = raw_bag.join()

        for stmt in self.stmts:
            raw_bag[StmtRawFilter(stmts=[stmt])].save(str(self.tmp_path / "raw" / stmt))
            joined_bag[StmtJoinedFilter(stmts=[stmt])].save(str(self.tmp_path / "joined" / stmt))

    def commit(self):
        """ we commit by renaming the tmp_path. """
        self.tmp_path.rename(self.filtered_path)
        return "success"

    def exception(self, exception) -> str:
        """ delete the temp folder. """
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        return f"failed {exception}"

    def __str__(self) -> str:
        return f"FilterTask(filtered_path: {self.filtered_path})"


class FilterProcess(AbstractProcess):

    def __init__(self,
                 parquet_dir: str,
                 filtered_dir: str,
                 file_type: str = "quarter",
                 stmts=None,
                 execute_serial: bool = False
                 ):
        super().__init__(execute_serial=execute_serial,
                         chunksize=0)
        self.stmts = ['BS', 'IS', 'CF', 'CP', 'CI', 'EQ']

        if stmts:
            self.stmts = stmts

        self.parquet_dir = parquet_dir
        self.filtered_dir = filtered_dir
        self.file_type = file_type

    def _get_present_parquet_files(self) -> List[str]:
        """
        returns the available folders within the parquet directory.
        Returns:
            List[str] list with foldernames.
        """

        parquet_folders = get_directories_in_directory(
            os.path.join(self.parquet_dir, self.file_type))

        # 2009q1.zip is actually empty and could issues during processing, so we  simply ignore it
        parquet_folders = [f for f in parquet_folders if f != "2009q1.zip"]

        return parquet_folders

    def _get_existing_filtered(self):
        return get_directories_in_directory(
            os.path.join(self.filtered_dir, self.file_type))

    def _delete_temp_folders(self):
        """
        remove any existing tmp folders (folders that were not successfully completed

        """
        dirs_in_filter_dir = get_directories_in_directory(
            os.path.join(self.filtered_dir, self.file_type))

        tmp_dirs = [d for d in dirs_in_filter_dir if d.startswith("tmp")]

        for tmp_dir in tmp_dirs:
            file_path = Path(self.filtered_dir) / self.file_type / tmp_dir
            shutil.rmtree(file_path, ignore_errors=True)

    def pre_process(self):
        self._delete_temp_folders()

    def calculate_tasks(self) -> List[Task]:
        existing = self._get_existing_filtered()
        available = self._get_present_parquet_files()

        missings = set(available) - set(existing)

        return [FilterTask(
            filtered_path=Path(self.filtered_dir) / self.file_type / missing,
            stmts=self.stmts)
            for missing in missings]
