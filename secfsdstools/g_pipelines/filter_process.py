"""
Defines pipeline steps that help with applying the same filter to mutilple subfolders
in parallel.
"""
import os
import shutil
from pathlib import Path
from typing import List, Callable

from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.c_automation.automation_utils import delete_temp_folders
from secfsdstools.c_automation.task_framework import Task, \
    AbstractProcessPoolProcess
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
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
    """
    Abstract FilterTask implementation providing some common basic implementations.
    Uses the ZipCollector to read the raw databags.
    """

    def __init__(self,
                 zip_file_name: str,
                 target_path: Path,
                 bag_type: str,  # raw or joined
                 stmts: List[str],
                 forms_filter=None,
                 post_load_filter: Callable[[RawDataBag], RawDataBag] = postloadfilter
                 ):
        """

        Args:
            zip_file_name: name of the source file that shall be readed by the  zipcollector
            target_path: path to store the filtered bag to
            bag_type: bag type (either "row" or "joined") to save the data as
            stmts: stmts to filter for ("BS", "IS", "CF", ...)
            forms_filter: defines which forms shall be loaded. default is ['10-K', '10-Q']
            post_load_filter: filter method to be applied after loading of the zip file.
                              default postloadfilter applies ReportPeriodRawFilter,
                              MainCoregRawFilter, USDOnlyRawFilter
        """
        if forms_filter is None:
            forms_filter = ['10-K', '10-Q']
        self.forms_filter = forms_filter
        self.post_load_filter = post_load_filter

        self.target_path = target_path
        self.stmts = stmts
        self.bag_type = bag_type

        self.zip_file_name = zip_file_name
        self.target_file_name = target_path.name
        self.tmp_path = target_path.parent / f"tmp_{self.target_file_name}"

    def commit(self):
        """
        we commit by renaming the tmp_path. This is an atomic action and either fails
        or succeeds. So if there is a tmp folder, we know that something failed and therefore,
        it is easy to recover and redo.
        """
        self.tmp_path.rename(self.target_path)
        return "success"

    def exception(self, exception) -> str:
        """ delete the temp folder. """
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        return f"failed {exception}"


class FilterTask(AbstractFilterTask):
    """
    Basic Filter implementation which applys filters for
    10-K and 10-Q forms, as well as
    ReportPeriodRawFilter, MainCoregRawFilter, and USDOnlyRawFilter
    """

    def prepare(self):
        """ prepare Task."""
        self.tmp_path.mkdir(parents=True, exist_ok=False)

    def execute(self):
        """
        Uses the ZipCollector to read the input data and then applys the filters for
        10-K and 10-Q forms, as well as applying the ReportPeriodRawFilter, MainCoregRawFilter,
        and USDOnlyRawFilter.
        Saves the result depending on the configuration either as raw or joined data bag

        """
        raw_bag = ZipCollector.get_zip_by_name(name=self.zip_file_name,
                                               forms_filter=self.forms_filter,
                                               stmt_filter=self.stmts,
                                               post_load_filter=self.post_load_filter).collect()

        if self.bag_type.lower() == "raw":
            raw_bag.save(str(self.tmp_path))
        elif self.bag_type.lower() == "joined":
            joined_bag = raw_bag.join()
            joined_bag.save(str(self.tmp_path))
        else:
            raise ValueError("bag_type must be either raw or joined")

    def __str__(self) -> str:
        return f"FilterTask(filtered_path: {self.target_path})"


class ByStmtFilterTask(AbstractFilterTask):
    """
    Basic Filter implementation which applys filters for
    10-K and 10-Q forms, as well as
    ReportPeriodRawFilter, MainCoregRawFilter, and USDOnlyRawFilter.

    Depending on the configuration, the results are either saved in raw or joined format.
    Moreover, the results are split up by stmt ("BS", "IS", "CF", ...) when being saved into
    own subfolders.
    """

    def prepare(self):
        """ prepare Task."""

        for stmt in self.stmts:
            (self.tmp_path / stmt).mkdir(parents=True, exist_ok=False)

    def execute(self):
        """
        Uses the ZipCollector to read the input data and then applys the filters for
        10-K and 10-Q forms, as well as applying the ReportPeriodRawFilter, MainCoregRawFilter,
        and USDOnlyRawFilter.
        Saves the result depending on the configuration either as raw or joined data bag.
        Moreover, splits the results up by stmt ("BS", "IS", "CF", ...).

        """
        raw_bag = ZipCollector.get_zip_by_name(name=self.zip_file_name,
                                               forms_filter=self.forms_filter,
                                               stmt_filter=self.stmts,
                                               post_load_filter=self.post_load_filter).collect()

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
        return f"ByStmtFilterTask(filtered_path: {self.target_path})"


class FilterProcess(AbstractProcessPoolProcess):
    """
    Applies basic filters on the raw indexed files and saves the result into the provided
    target_path.
    Does it per zip-file and can do it in parallel, depending on the parameter settings.

    Applies the basic filters ReportPeriodRawFilter, MainCoregRawFilter, USDOnlyRawFilter.
    """

    def __init__(self,
                 db_dir: str,
                 target_dir: str,
                 bag_type: str,  # raw or joined
                 file_type: str = "quarter",
                 save_by_stmt: bool = False,
                 stmts=None,
                 execute_serial: bool = False,
                 forms_filter=None,
                 post_load_filter: Callable[[RawDataBag], RawDataBag] = postloadfilter
                 ):
        """
        Constructor.
        Args:
            db_dir: directory of the sqlite-db file. Used to read the available zipfiles.
            target_dir: directory to where the results have to be written
            bag_type: either "raw" or "joined" and defines what bag-type shall be written
            file_type: the file type to be processed, default is "quarter"
            save_by_stmt: Flag to indicate whether the results should be split up by the stmt.
                          If it is true, subfolders for every stmt "BS", "IS", "CF", ... will be
                          created.
            stmts: The list of stmts that should be filtered: "BS", "IS", "CF", ... or none
            execute_serial: Flag to indicate whether the files should be process in serial manner.
        """
        super().__init__(execute_serial=execute_serial,
                         chunksize=0)
        if forms_filter is None:
            forms_filter = ['10-K', '10-Q']
        self.forms_filter = forms_filter
        self.post_load_filter = post_load_filter

        self.stmts = ['BS', 'IS', 'CF', 'CP', 'CI', 'EQ']

        if stmts:
            self.stmts = stmts

        self.dbaccessor = ParquetDBIndexingAccessor(db_dir=db_dir)

        self.target_dir = target_dir
        self.file_type = file_type
        self.bag_type = bag_type
        self.save_by_stmt = save_by_stmt

    def _get_existing_filtered(self):
        return get_directories_in_directory(
            os.path.join(self.target_dir, self.file_type))

    def pre_process(self):
        delete_temp_folders(root_path=Path(self.target_dir) / self.file_type)

    def calculate_tasks(self) -> List[Task]:
        existing = self._get_existing_filtered()
        available = self.dbaccessor.read_filenames_by_type(originFileType=self.file_type)

        missings = set(available) - set(existing)
        if self.save_by_stmt:
            return [ByStmtFilterTask(
                zip_file_name=missing,
                target_path=Path(self.target_dir) / self.file_type / missing,
                stmts=self.stmts,
                bag_type=self.bag_type,
                forms_filter=self.forms_filter,
                post_load_filter=self.post_load_filter
            )
                for missing in missings]
        else:
            return [FilterTask(
                zip_file_name=missing,
                target_path=Path(self.target_dir) / self.file_type / missing,
                stmts=self.stmts,
                bag_type=self.bag_type,
                forms_filter=self.forms_filter,
                post_load_filter=self.post_load_filter
            )
                for missing in missings]
