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
from typing import List

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_automation.task_framework import AbstractProcess
from secfsdstools.g_pipelines.standardize_process import StandardizeProcess

CURRENT_DIR, _ = os.path.split(__file__)


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
