import os
import shutil
from pathlib import Path
from typing import List

from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.c_automation.task_framework import AbstractProcess, Task, delete_temp_folders
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


class AbstractFilterTask:

    def __init__(self,
                 filtered_path: Path,
                 bag_type: str,  # raw or joined
                 stmts: List[str]):
        self.filtered_path = filtered_path
        self.stmts = stmts
        self.bag_type = bag_type

        self.file_name = filtered_path.name
        self.tmp_path = filtered_path.parent / f"tmp_{self.file_name}"

    def commit(self):
        """ we commit by renaming the tmp_path. """
        self.tmp_path.rename(self.filtered_path)
        return "success"

    def exception(self, exception) -> str:
        """ delete the temp folder. """
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        return f"failed {exception}"


class FilterTask(AbstractFilterTask):

    def prepare(self):
        """ prepare Task."""

        self.tmp_path.mkdir(parents=True, exist_ok=False)

    def execute(self):
        raw_bag = ZipCollector.get_zip_by_name(name=self.file_name,
                                               forms_filter=['10-K', '10-Q'],
                                               stmt_filter=self.stmts,
                                               post_load_filter=postloadfilter).collect()

        if self.bag_type.lower() == "raw":
            raw_bag.save(str(self.tmp_path))
        elif self.bag_type.lower() == "joined":
            joined_bag = raw_bag.join()
            joined_bag.save(str(self.tmp_path))
        else:
            raise ValueError("bag_type must be either raw or joined")

    def __str__(self) -> str:
        return f"FilterTask(filtered_path: {self.filtered_path})"


class ByStmtFilterTask(AbstractFilterTask):

    def prepare(self):
        """ prepare Task."""

        for stmt in self.stmts:
            (self.tmp_path / stmt).mkdir(parents=True, exist_ok=False)

    def execute(self):
        raw_bag = ZipCollector.get_zip_by_name(name=self.file_name,
                                               forms_filter=['10-K', '10-Q'],
                                               stmt_filter=self.stmts,
                                               post_load_filter=postloadfilter).collect()

        if self.bag_type.lower() == "raw":
            self._execute_raw(raw_bag)
        elif self.bag_type.lower() == "joined":
            self._execute_joined(raw_bag)
        else:
            raise ValueError("bag_type must be either raw or joined")

    def _execute_raw(self, raw_bag: RawDataBag):
        for stmt in self.stmts:
            raw_bag[StmtRawFilter(stmts=[stmt])].save(str(self.tmp_path / stmt))

    def _execute_joined(self, raw_bag: RawDataBag):
        joined_bag = raw_bag.join()

        for stmt in self.stmts:
            joined_bag[StmtJoinedFilter(stmts=[stmt])].save(str(self.tmp_path / stmt))

    def __str__(self) -> str:
        return f"ByStmtFilterTask(filtered_path: {self.filtered_path})"


class FilterProcess(AbstractProcess):

    def __init__(self,
                 parquet_dir: str,
                 target_dir: str,
                 bag_type: str,  # raw or joined
                 file_type: str = "quarter",
                 save_by_stmt: bool = False,
                 stmts=None,
                 execute_serial: bool = False
                 ):
        super().__init__(execute_serial=execute_serial,
                         chunksize=0)
        self.stmts = ['BS', 'IS', 'CF', 'CP', 'CI', 'EQ']

        if stmts:
            self.stmts = stmts

        self.parquet_dir = parquet_dir
        self.target_dir = target_dir
        self.file_type = file_type
        self.bag_type = bag_type
        self.save_by_stmt = save_by_stmt

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
            os.path.join(self.target_dir, self.file_type))

    def pre_process(self):
        delete_temp_folders(root_path=Path(self.target_dir) / self.file_type)

    def calculate_tasks(self) -> List[Task]:
        existing = self._get_existing_filtered()
        available = self._get_present_parquet_files()

        missings = set(available) - set(existing)
        if self.save_by_stmt:
            return [ByStmtFilterTask(
                filtered_path=Path(self.target_dir) / self.file_type / missing,
                stmts=self.stmts,
                bag_type=self.bag_type
            )
                for missing in missings]
        else:
            return [FilterTask(
                filtered_path=Path(self.target_dir) / self.file_type / missing,
                stmts=self.stmts,
                bag_type=self.bag_type
            )
                for missing in missings]
