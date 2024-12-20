"""
Module that contains the AbstractProcess implementation that can standardize
JoinedDataBags for BalanceSheet, IncomeStatement, and  CashFlow.
"""
import shutil
from pathlib import Path
from typing import List

from secfsdstools.c_automation.automation_utils import delete_temp_folders
from secfsdstools.c_automation.task_framework import Task, \
    CheckByTimestampMergeBaseTask, AbstractThreadProcess
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer
from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer


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
            root_path=root_path,
            pathfilter="*",  # can actually be ignored
            target_path=target_path
        )

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

        """
        bs_standardizer = BalanceSheetStandardizer()
        is_standardizer = IncomeStatementStandardizer()
        cf_standardizer = CashFlowStandardizer()

        # standardize bs
        joined_bag_bs = JoinedDataBag.load(str(self.root_path / "BS"))
        bs_standardizer.process(joined_bag_bs.pre_num_df.copy())
        bs_standardizer.get_standardize_bag().save(str(tmp_path / "BS"))

        # standardize is
        joined_bag_is = JoinedDataBag.load(str(self.root_path / "IS"))
        is_standardizer.process(joined_bag_is.pre_num_df.copy())
        is_standardizer.get_standardize_bag().save(str(tmp_path / "IS"))

        # standardize cf
        joined_bag_cf = JoinedDataBag.load(str(self.root_path / "CF"))
        cf_standardizer.process(joined_bag_cf.pre_num_df.copy())
        cf_standardizer.get_standardize_bag().save(str(tmp_path / "CF"))


class StandardizeProcess(AbstractThreadProcess):
    """
    Expects subfolders BS, IS, CF inside the provided root_dir.
    The data has to be provided as JoinedDataBags.

    The resulting StandardizedBags are stored under the subfolders
    BS, IS, and CF unter the target_dir.

    Will be executed if anything had changed (modification timestamp) in the
    root_dir since last execution.
    """

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
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
        super().__init__(execute_serial=False,
                         chunksize=0)
        self.root_dir = root_dir
        self.target_dir = target_dir

    def pre_process(self):
        """
        Deleting any tempfolder, if a previous execution did fail.
        """
        delete_temp_folders(root_path=Path(self.target_dir))

    def calculate_tasks(self) -> List[Task]:
        """
        Prepares the StandardizerTask which will actually execute the standardization.
        """
        task = StandardizerTask(
            root_path=Path(self.root_dir),
            target_path=Path(self.target_dir)
        )

        # since this is a one task process, we just check if there is really something to do
        if len(task.paths_to_process) > 0:
            return [task]
        return []
