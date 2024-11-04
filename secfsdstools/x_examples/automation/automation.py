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
from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.c_automation.task_framework import AbstractProcess, Task, delete_temp_folders
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer
from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer

CURRENT_DIR, _ = os.path.split(__file__)


class StandardizerTask:

    def __init__(self,
                 root_path: Path,
                 target_path: Path):
        self.root_path = root_path

        self.target_path = target_path
        self.dir_name = target_path.name
        self.tmp_path = target_path.parent / f"tmp_{self.dir_name}"

    def prepare(self):
        (self.tmp_path / "BS").mkdir(parents=True, exist_ok=False)
        (self.tmp_path / "IS").mkdir(parents=True, exist_ok=False)
        (self.tmp_path / "CF").mkdir(parents=True, exist_ok=False)

    def commit(self):
        """ we commit by renaming the tmp_path. """
        self.tmp_path.rename(self.target_path)
        return "success"

    def exception(self, exception) -> str:
        """ delete the temp folder. """
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        return f"failed {exception}"

    def __str__(self) -> str:
        return (f"StandardizerTask(root_path: {self.root_path}, "
                f"target_path: {self.target_path})")

    def execute(self):
        bs_standardizer = BalanceSheetStandardizer()
        is_standardizer = IncomeStatementStandardizer()
        cf_standardizer = CashFlowStandardizer()

        # load data
        joined_bag = JoinedDataBag.load(str(self.root_path))

        # standardize bs
        # bs_standardizer.process(joined_bag.pre_num_df.copy())
        # bs_standardizer.get_standardize_bag().save(str(self.tmp_path / "BS"))

        # standardize is
        is_standardizer.process(joined_bag.pre_num_df.copy())
        is_standardizer.get_standardize_bag().save(str(self.tmp_path / "IS"))

        # standardize cf
        cf_standardizer.process(joined_bag.pre_num_df.copy())
        cf_standardizer.get_standardize_bag().save(str(self.tmp_path / "CF"))


class StandardizeProcess(AbstractProcess):

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
                 file_type: str = "quarter",
                 execute_serial: bool = False
                 ):
        super().__init__(execute_serial=execute_serial,
                         chunksize=0)
        self.root_dir = root_dir
        self.target_dir = target_dir
        self.file_type = file_type

    def _get_available_filtered(self):
        return get_directories_in_directory(
            os.path.join(self.root_dir, self.file_type))

    def _get_standardized(self):
        return get_directories_in_directory(
            os.path.join(self.target_dir, self.file_type))

    def pre_process(self):
        delete_temp_folders(root_path=Path(self.target_dir) / self.file_type)

    def calculate_tasks(self) -> List[Task]:
        filtered = self._get_available_filtered()
        standardized = self._get_standardized()

        missings = set(filtered) - set(standardized)

        return [StandardizerTask(
            root_path=Path(self.root_dir) / self.file_type / missing,
            target_path=Path(self.target_dir) / self.file_type / missing
        ) for missing in missings]


def define_extra_processes(config: Configuration) -> List[AbstractProcess]:
    from secfsdstools.x_examples.automation.filter_process import FilterProcess
    from secfsdstools.x_examples.automation.combine_process import CombineProcess

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
                       bag_type="joined"
                       ),

        StandardizeProcess(root_dir=joined_dir,
                           target_dir=standardized_dir,
                           execute_serial=False)

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
