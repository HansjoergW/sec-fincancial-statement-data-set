"""
This module shows the automation to add additional steps after the usual update process
(which is downloading new zip files, transforming them to parquet, indexding them).

You can configure this function in the secfsdstools configuration file, by adding
a postupdateprocesses definition. For instance, if you want to use this example,
just add the postupdateprocesses definition as shown below:

<pre>
[DEFAULT]
downloaddirectory = ...
dbdirectory = ...
parquetdirectory = ...
useragentemail = ...
autoupdate = True
keepzipfiles = False
postupdateprocesses=secfsdstools.x_examples.automation.automation.define_extra_processes
</pre>

If you want to use it, you also need to add additional configuration entries as shown below:

<pre>
[Filter]
filtered_dir_by_stmt_joined = C:/data/sec/automated/_1_filtered_by_stmt_joined

[Concat]
concat_dir_by_stmt_joined = C:/data/sec/automated/_2_concat_by_stmt_joined

[Standardizer]
standardized_dir = C:/data/sec/automated/_3_standardized

; [SingleBag]
; singlebag_dir = C:/data/sec/automated/_4_single_bag
</pre>

(A complete configuration file using the "define_extra_processes" function is available in the file
 automation_config.cfg which is in the same package as this module here.)

This example adds 4 main steps to the usual updated process.

First, it creates a joined bag for every zip file, filters it for 10-K and 10-Q reports only
and also applies the filters  ReportPeriodRawFilter, MainCoregRawFilter, USDOnlyRawFilter,
OfficialTagsOnlyRawFilter. The filtered joined bag is stored under the path defined as
filtered_dir_by_stmt_joined. Furthermore, the data will also be split by stmt.

Second, it creates a single joined bag for every statement (balance sheet, income statement,
cash flow, cover page, ...) that contains the data from all zip files, resp from all the
available quarters. These bags are stored under the path defined as concat_dir_by_stmt_joined.

Third, it standardizes the data for balance sheet, income statement, and cash flow and stores
the standardized bags under the path that is defined as standardized_dir.

The fourth step is optional and is only executed if the configuration file contains an entry
for singlebag_dir. If it does, it will create a single joined bag concatenating all the bags
created in the second step, so basically creating a single bag that contains all the data from
all the available zip files, resp. quarters.

All this steps use basic implementations of the AbstractProcess class from the
secfsdstools.g_pipeline package.

Furthermore, all these steps check if something changed since the last run and are only executed
if something did change (for instance, if a new zip file became available).

Have also a look at the notebook 08_00_automation_basics.

"""
from typing import List

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_automation.task_framework import AbstractProcess
from secfsdstools.g_pipelines.concat_process import ConcatByNewSubfoldersProcess, \
    ConcatByChangedTimestampProcess
from secfsdstools.g_pipelines.filter_process import FilterProcess
from secfsdstools.g_pipelines.standardize_process import StandardizeProcess


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
    4. optional and only executed if the singlebag_dir is configured in the configuration.
       it concats all the bags from step 2 together into a single bag.

    Please have a look at the notebook 08_00_automation_basics for further details.

    Args:
        configuration: the configuration

    Returns:
        List[AbstractProcess]: List with the defined process steps

    """

    joined_by_stmt_dir = configuration.config_parser.get(section="Filter",
                                                         option="filtered_dir_by_stmt_joined")

    concat_by_stmt_dir = configuration.config_parser.get(section="Concat",
                                                         option="concat_dir_by_stmt_joined")

    standardized_dir = configuration.config_parser.get(section="Standardizer",
                                                       option="standardized_dir")

    singlebag_dir = configuration.config_parser.get(section="SingleBag",
                                                    option="singlebag_dir",
                                                    fallback="")

    processes: List[AbstractProcess] = []

    processes.append(
        # 1. Filter, join, and save by stmt
        FilterProcess(db_dir=configuration.db_dir,
                      target_dir=joined_by_stmt_dir,
                      bag_type="joined",
                      save_by_stmt=True,
                      execute_serial=configuration.no_parallel_processing
                      )
    )

    processes.extend([
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
                                     )
    ])

    processes.append(
        # 3. Standardize the data
        StandardizeProcess(root_dir=f"{concat_by_stmt_dir}",
                           target_dir=standardized_dir),
    )

    # 4. create a single joined bag with all the data, if it is defined
    if singlebag_dir != "":
        processes.append(
            ConcatByChangedTimestampProcess(
                root_dir=f"{concat_by_stmt_dir}/",
                target_dir=f"{singlebag_dir}/all",
            )
        )

    return processes
