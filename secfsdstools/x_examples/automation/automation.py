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

In this example, we have a configuration file "automation_config.cfg" which we are going
to use in the __main__ section of this module.
"""
import logging
import os
from typing import List

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_automation.task_framework import AbstractProcess

CURRENT_DIR, _ = os.path.split(__file__)


# pylint: disable=C0415
def define_extra_processes(configuration: Configuration) -> List[AbstractProcess]:
    """
    example definition of an additional pipeline.
    It adds process steps that:
    1. Filter for 10-K and 10-Q reports, als apply the filters
       ReportPeriodRawFilter, MainCoregRawFilter, USDOnlyRawFilter, OfficialTagsOnlyRawFilter,
       then joins the data and splits up the data by stmt (BS, IS, CF, ...)
       This is done for every zipfile individually
    2. concats all the stmts together, so that there is one file for every stmt containing all
       the available data
    3. standardizing the data for BS, IS, CF


    Args:
        configuration: the configuration

    Returns:
        List[AbstractProcess]: List with the defined process steps

    """
    # Attention: imports that import secfsdstools classes need to be inside the function,
    # otherwise, circular dependencies might occur due to the automated update process.

    from secfsdstools.g_pipelines.filter_process import FilterProcess
    from secfsdstools.g_pipelines.concat_process import ConcatByNewSubfoldersProcess, \
        ConcatByChangedTimestampProcess
    from secfsdstools.g_pipelines.standardize_process import StandardizeProcess


    joined_by_stmt_dir = configuration.config_parser.get(section="Filter",
                                                         option="filtered_dir_by_stmt_joined")

    concat_by_stmt_dir = configuration.config_parser.get(section="Concat",
                                                         option="concat_dir_by_stmt_joined")

    standardized_dir = configuration.config_parser.get(section="Standardizer",
                                                       option="standardized_dir")

    singlebag_dir = configuration.config_parser.get(section="SingleBag",
                                                    option="singlebag_dir")

    return [
        # 1. Filter, join, and save by stmt
        FilterProcess(db_dir=configuration.db_dir,
                      target_dir=joined_by_stmt_dir,
                      bag_type="joined",
                      save_by_stmt=True,
                      execute_serial=configuration.no_parallel_processing
                      ),

        # 2. building datasets with all entries by stmt
        ConcatByNewSubfoldersProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_by_stmt_dir}/BS",
                                     pathfilter="*/BS"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_by_stmt_dir}/CF",
                                     pathfilter="*/CF"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_by_stmt_dir}/CI",
                                     pathfilter="*/CI"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_by_stmt_dir}/CP",
                                     pathfilter="*/CP"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_by_stmt_dir}/EQ",
                                     pathfilter="*/EQ"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_by_stmt_dir}/IS",
                                     pathfilter="*/IS"
                                     ),

        # 3. Standardize the data
        StandardizeProcess(root_dir=f"{concat_by_stmt_dir}",
                           target_dir=standardized_dir),

        # 4. create a single joined bag with all the data
        ConcatByChangedTimestampProcess(
            root_dir=f"{concat_by_stmt_dir}/",
            target_dir=f"{singlebag_dir}/all",
        )
    ]


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
