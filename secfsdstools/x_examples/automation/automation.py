"""
This module shows the automation possibilities.
These are mainly providing a hook method, which can create additional steps that are executed
during the "check-for-update" (checking whether a new quarterly zip file is available), and
providing another hook-method, that is simply called at the end of the whole update process.

As a remainder, the framework executes all update steps (check and downloading new zip files,
transforming them to parquet, indexing the reports, and any additional steps that you define
as shown here.

Usually, you can configure these methods here in the secfsdstools configuration file, like
<pre>
[DEFAULT]
downloaddirectory = ...
dbdirectory = ...
parquetdirectory = ...
useragentemail = ...
autoupdate = True
keepzipfiles = False
postppdatehook=secfsdstools.x_examples.automation.automation.after_update
postupdateprocesses=secfsdstools.x_examples.automation.automation.define_extra_processes
</pre>

In this example, we have a config file "automation_config.cfg" which we are going to use in the
__main__ section of this module.
"""
import logging
import os
import shutil
from pathlib import Path
from typing import List

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_automation.task_framework import AbstractProcess, Task, delete_temp_folders, \
    BaseTask
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_filter.joinedfiltering import OfficialTagsOnlyJoinedFilter
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer
from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer

CURRENT_DIR, _ = os.path.split(__file__)


class StandardizerTask(BaseTask):

    def __init__(self,
                 root_path: Path,
                 target_path: Path,
                 ):
        super().__init__(
            root_path=root_path,
            filter="*",  # can actually be ignored
            target_path=target_path,
            check_by_timestamp=True  # alwas true
        )

    def prepare(self):
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

    def do_execution(self, paths_to_process: List[Path]):
        """
        Args:
            paths_to_process: can actually be ignored in this context, since we know which
            folders have to processed (BS, IS, CF)

        Returns:
        """
        bs_standardizer = BalanceSheetStandardizer()
        is_standardizer = IncomeStatementStandardizer()
        cf_standardizer = CashFlowStandardizer()

        # standardize bs
        joined_bag_bs = (
            JoinedDataBag.load(str(self.root_path / "BS"))
            [OfficialTagsOnlyJoinedFilter()])
        bs_standardizer.process(joined_bag_bs.pre_num_df.copy())
        bs_standardizer.get_standardize_bag().save(str(self.tmp_path / "BS"))

        # standardize is
        joined_bag_is = (
            JoinedDataBag.load(str(self.root_path / "IS"))
            [OfficialTagsOnlyJoinedFilter()])
        is_standardizer.process(joined_bag_is.pre_num_df.copy())
        is_standardizer.get_standardize_bag().save(str(self.tmp_path / "IS"))

        # standardize cf
        joined_bag_cf = (
            JoinedDataBag.load(str(self.root_path / "CF"))
            [OfficialTagsOnlyJoinedFilter()])
        cf_standardizer.process(joined_bag_cf.pre_num_df.copy())
        cf_standardizer.get_standardize_bag().save(str(self.tmp_path / "CF"))


class StandardizeProcess(AbstractProcess):
    """
    Expects subfolders BS, IS, CF inside the provided root_dir.
    """

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
                 ):
        super().__init__(execute_serial=False,
                         chunksize=0)
        self.root_dir = root_dir
        self.target_dir = target_dir

    def pre_process(self):
        delete_temp_folders(root_path=Path(self.target_dir))

    def calculate_tasks(self) -> List[Task]:
        task = StandardizerTask(
            root_path=Path(self.root_dir),
            target_path=Path(self.target_dir)
        )

        # since this is a one task process, we just check if there is really something to do
        if len(task.paths_to_process) > 0:
            return [task]
        return []


def define_extra_processes(config: Configuration) -> List[AbstractProcess]:
    from secfsdstools.g_pipelines.filter_process import FilterProcess
    from secfsdstools.g_pipelines.combine_process import CombineProcess

    raw_dir = config.config_parser.get(section="Filter",
                                       option="filtered_dir_raw")
    joined_dir = config.config_parser.get(section="Filter",
                                          option="filtered_dir_joined")
    joined_by_stmt_dir = config.config_parser.get(section="Filter",
                                                  option="filtered_dir_by_stmt_joined")
    standardized_dir = config.config_parser.get(section="Standardizer",
                                                option="standardized_dir")

    return [
        # raw
        FilterProcess(parquet_dir=config.parquet_dir,
                      target_dir=raw_dir,
                      bag_type="raw",
                      save_by_stmt=False,
                      execute_serial=False  # switch to true in case of memory problems
                      ),
        CombineProcess(root_dir=f"{raw_dir}/quarter",
                       target_dir=f"{raw_dir}/all",
                       bag_type="raw"
                       ),

        # joined
        FilterProcess(parquet_dir=config.parquet_dir,
                      target_dir=joined_dir,
                      bag_type="joined",
                      save_by_stmt=False,
                      execute_serial=False  # switch to true in case of memory problems
                      ),
        CombineProcess(root_dir=f"{joined_dir}/quarter",
                       target_dir=f"{joined_dir}/all",
                       bag_type="joined"
                       ),

        # joined by stmt
        FilterProcess(parquet_dir=config.parquet_dir,
                      target_dir=f"{joined_by_stmt_dir}",
                      bag_type="joined",
                      save_by_stmt=True,
                      stmts=["BS", "CF", "CI", "CP", "EQ", "IS"],
                      execute_serial=False,  # switch to true in case of memory problems
                      ),

        # building datasets with all entries by stmt
        CombineProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                       target_dir=f"{joined_by_stmt_dir}/all_by_stmt/BS",
                       filter="*/BS",
                       bag_type="joined"
                       ),
        CombineProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                       target_dir=f"{joined_by_stmt_dir}/all_by_stmt/CF",
                       filter="*/CF",
                       bag_type="joined"
                       ),
        CombineProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                       target_dir=f"{joined_by_stmt_dir}/all_by_stmt/CI",
                       filter="*/CI",
                       bag_type="joined"
                       ),
        CombineProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                       target_dir=f"{joined_by_stmt_dir}/all_by_stmt/CP",
                       filter="*/CP",
                       bag_type="joined"
                       ),
        CombineProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                       target_dir=f"{joined_by_stmt_dir}/all_by_stmt/EQ",
                       filter="*/EQ",
                       bag_type="joined"
                       ),
        CombineProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                       target_dir=f"{joined_by_stmt_dir}/all_by_stmt/IS",
                       filter="*/IS",
                       bag_type="joined"
                       ),

        # building an all dataset based on the all-by-stmt datasets
        CombineProcess(root_dir=f"{joined_by_stmt_dir}/all_by_stmt",
                       target_dir=f"{joined_by_stmt_dir}/all",
                       bag_type="joined",
                       check_by_timestamp=True
                       ),

        StandardizeProcess(root_dir=f"{joined_by_stmt_dir}/all_by_stmt",
                           target_dir=standardized_dir)

    ]


def after_update(config: Configuration):
    pass


if __name__ == '__main__':
    from secfsdstools.a_config.configmgt import ConfigurationManager, SECFSDSTOOLS_ENV_VAR_NAME
    from secfsdstools.c_update.updateprocess import Updater

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    # define which configuration file to use
    os.environ[SECFSDSTOOLS_ENV_VAR_NAME] = f"{CURRENT_DIR}/automation_config.cfg"

    config = ConfigurationManager.read_config_file()

    updater = Updater.get_instance(config)

    # We call this method mainly for demonstration purpose. Therefore, we also set
    # force_update=True, so that update process is being executed, regardless if the last
    # update process run less than 24 hours before.

    # You could also just start to use any feature of the framework. This would also trigger the
    # update process to run, but at most once every 24 hours.
    updater.update(force_update=True)
