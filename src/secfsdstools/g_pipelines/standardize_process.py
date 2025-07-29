"""
Module that contains the AbstractProcess implementation that can standardize
JoinedDataBags for BalanceSheet, IncomeStatement, and  CashFlow.
"""
import gc
import logging
import shutil
from pathlib import Path
from typing import List

from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.c_automation.automation_utils import delete_temp_folders
from secfsdstools.c_automation.task_framework import AbstractThreadProcess, CheckByTimestampMergeBaseTask, Task
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer
from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer

LOGGER = logging.getLogger(__name__)


def _bs_standardization(root_path: Path, tmp_path: Path):
    # standardize bs
    bs_standardizer = BalanceSheetStandardizer()

    logging.info("create standardized BS dataset")
    joined_bag_bs = JoinedDataBag.load(str(root_path / "BS"))
    bs_standardizer.process(joined_bag_bs.pre_num_df)
    bs_standardizer.get_standardize_bag().save(str(tmp_path / "BS"))


def _is_standardization(root_path: Path, tmp_path: Path):
    # standardize is
    is_standardizer = IncomeStatementStandardizer()

    logging.info("create standardized IS dataset")
    joined_bag_is = JoinedDataBag.load(str(root_path / "IS"))
    is_standardizer.process(joined_bag_is.pre_num_df)
    is_standardizer.get_standardize_bag().save(str(tmp_path / "IS"))


def _cf_standardization(root_path: Path, tmp_path: Path):
    # standardize cf
    cf_standardizer = CashFlowStandardizer()

    logging.info("create standardized CF dataset")
    joined_bag_cf = JoinedDataBag.load(str(root_path / "CF"))
    cf_standardizer.process(joined_bag_cf.pre_num_df)
    cf_standardizer.get_standardize_bag().save(str(tmp_path / "CF"))


class StandardizerTask(CheckByTimestampMergeBaseTask):
    """
    Standardizes the content in the root_path and writes the StandardizedBags
    into the target_path.

    The root_path is expected to contain the subfolders BS, IS, and CF.
    The data has to be present as JoinedDataBags.
    """

    def __init__(self,
                 root_path: Path,
                 target_path: Path,
                 ):
        """
        Task that creates the standardized datasets for BS, IS, and CF.
        Expects the data already filtered as subfolders BS, IS, and CF in root_path.

        Args:
            root_path: The root path, containing subfolders for BS, IS, and CF.
                       The Data has to be provided as standardized DataBags.
            target_path: the target path to write the results to
        """
        super().__init__(
            root_paths=[root_path],
            pathfilter="*",  # can actually be ignored
            target_path=target_path
        )
        self.root_path = root_path

    def prepare(self):
        """
        Preparing the tmp_paths by creating the necessary subfolders.
        """
        (self.tmp_path / "BS").mkdir(parents=True, exist_ok=False)
        (self.tmp_path / "IS").mkdir(parents=True, exist_ok=False)
        (self.tmp_path / "CF").mkdir(parents=True, exist_ok=False)

    def exception(self, exception) -> str:
        """ delete the temp folder. """
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        return f"failed {exception}"

    def __str__(self) -> str:
        return (f"StandardizerTask(root_path: {self.root_path}, "
                f"target_path: {self.target_path})")

    def do_execution(self,
                     paths_to_process: List[Path],
                     tmp_path: Path):
        """
        executes the actual standardization, by loading the data for
        BS, IS, and CF and producing saving the StandardizedBag.
        Does apply the OfficialTagsOnlyJoinedFilter before standardizing.

        Args:
            paths_to_process: can actually be ignored in this context, since we know which
            folders have to processed (BS, IS, CF)
            tmp_path: target path to write the result to
        """
        _bs_standardization(root_path=self.root_path, tmp_path=tmp_path)
        gc.collect()
        _is_standardization(root_path=self.root_path, tmp_path=tmp_path)
        gc.collect()
        _cf_standardization(root_path=self.root_path, tmp_path=tmp_path)
        gc.collect()


class StandardizeProcess(AbstractThreadProcess):
    """
    Expects subfolders BS, IS, CF directly inside the provided root_dir
    or in all subfolders of the root_dir.

    The data has to be provided as JoinedDataBags.

    The resulting StandardizedBags are stored under the subfolders
    BS, IS, and CF unter the target_dir, resp. inside additional subfolders.

    Will be executed if anything has changed (modification timestamp) in the
    root_dir since last execution.
    """

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
                 execute_serial=True
                 ):
        """
        Expects subfolders BS, IS, CF inside the provided root_dir.
        The data has to be provided as JoinedDataBags.

        The resulting StandardizedBags are stored under the subfolders
        BS, IS, and CF unter the target_dir.

        Will be executed if anything had changed (modification timestamp) in the
        root_dir since last execution.

        Args:
            root_dir: folder containing subfolders BS, IS, and CF which have to provide
            the data as JoinedDataBags
            target_dir: directory to write the resulting StandardizedBags to
        """
        super().__init__(execute_serial=execute_serial,
                         chunksize=0)
        self.root_dir = root_dir
        self.target_dir = target_dir

    def pre_process(self):
        """
        Deleting any tempfolder, if a previous execution did fail.
        """
        delete_temp_folders(root_path=Path(self.target_dir))

    def _check_if_multiple_sub_dirs(self):
        """
        When we find directly BS, CF, IS subfolders, we just need to process the root folder.
        If we don't find these subfolders, we expect that every subfolder must be processed
        individually.
        """
        return not ((Path(self.root_dir) / "BS").exists() and
                    (Path(self.root_dir) / "CF").exists() and
                    (Path(self.root_dir) / "IS").exists())

    def calculate_tasks(self) -> List[Task]:
        """
        Prepares the StandardizerTask which will actually execute the standardization.
        """
        if self._check_if_multiple_sub_dirs():
            # we need to process all subfolders of the root_dir
            existing = get_directories_in_directory(self.target_dir)
            available = get_directories_in_directory(self.root_dir)

            not_standardized_folders = set(available) - set(existing)

            tasks = [StandardizerTask(
                root_path=Path(self.root_dir) / missing,
                target_path=Path(self.target_dir) / missing
            ) for missing in not_standardized_folders]

            return tasks

        # the root dir directly contains the BS, IS, and CF folder
        # so, we just have one task to create
        task = StandardizerTask(
            root_path=Path(self.root_dir),
            target_path=Path(self.target_dir)
        )

        # since this is a one task process, we just check if there is really something to do
        if len(task.paths_to_process) > 0:
            return [task]
        return []
