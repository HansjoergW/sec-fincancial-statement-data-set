# pylint: disable=C0301
"""
This module shows the automation to add additional steps after the usual update process
(which is downloading new zip files, transforming them to parquet, indexding them).

This example has a low memory footprint, so it is safe to use also on systems with 16GB
of memory.

If you update the configuration as defined below, you will get the following datasets:
- single filtered and joined bags for every stmt (BS, IS, CF, ..) containing the data from
  all available zip files.
- a single filtered and joined bag containing the data from all available zip files.
- single standardized bags for BS, IS, CF containing the data from all available zip files.

Moreover, these file will be automatically updated as soon as a new zip file becomes available
at the SEC website.

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
postupdateprocesses=secfsdstools.x_examples.automation.memory_optimized_automation.define_extra_processes
</pre>

If you want to use it, you also need to add additional configuration entries as shown below:

<pre>
[Filter]
filtered_joined_by_stmt_dir = C:/data/sec/automated/_1_by_quarter/_1_filtered_joined_by_stmt
parallelize = True

[Standardizer]
standardized_by_stmt_dir = C:/data/sec/automated/_1_by_quarter/_2_standardized_by_stmt

[Concat]
concat_joined_by_stmt_dir = C:/data/sec/automated/_2_all/_1_joined_by_stmt
concat_joined_all_dir = C:/data/sec/automated/_2_all/_2_joined
concat_standardized_by_stmt_dir = C:/data/sec/automated/_2_all/_3_standardized_by_stmt

(A complete configuration file using the "define_extra_processes" function is available in the file
 memory_optimized_automation_config.cfg which is in the same package as this module here.)

This example adds 5 main steps to the usual updated process.

First, it creates a joined bag for every zip file, filters it for 10-K and 10-Q reports only
and also applies the filters  ReportPeriodRawFilter, MainCoregRawFilter, USDOnlyRawFilter,
OfficialTagsOnlyRawFilter. The filtered joined bag is stored under the path defined as
filtered_dir_by_stmt_joined. Furthermore, the data will also be split by stmt.
This data will be stored under the path defined as `filtered_joined_by_stmt_dir`.
Note: setting "parallelize" in the config to False, well be slower in the initial loading
but using less memory.

Second, it produces standardized bags for BS, IS, CF for every zip file based on the filtered
data from the previous step. These bags are stored under the path defined as
`standardized_by_stmt_dir`.

Third, it creates a single joined bag for every statement (balance sheet, income statement,
cash flow, cover page, ...) based on the filtered data from the first step.
These bags are stored under the path defined as `concat_joined_by_stmt_dir`.

Fourth, it will create a single with all data from all different statements, by merging the
bags from the previous step into a single big. This bag will be stored under the path defined
as `concat_standardized_by_stmt_dir`.

Fifth, it will create single standardized bags for BS, IS, CF from the results in the second step.
These bags will be stored under the path defined as `concat_standardized_by_stmt_dir`.

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
       This is done for every zipfile individually.
    2. it produces a standardize bag for every quarter/zipfile.
    3. it concatenates all the stmts together, so that there is one file for every stmt containing all
       the available data
    4. it creates a single bag containing all the filtered and joined data
    5. it creates a standardize bag for BS, IS, and CF containing all the available data for stmt
       in one single file

    All these steps have a low memory footprint, so, the should run without any problems also
    on 16 GB machine.

    Please have a look at the notebook 08_00_automation_basics for further details.

    Args:
        configuration: the configuration

    Returns:
        List[AbstractProcess]: List with the defined process steps

    """

    filtered_joined_by_stmt_dir = configuration.config_parser.get(
        section="Filter",
        option="filtered_joined_by_stmt_dir")

    filter_parallelize = configuration.config_parser.get(
        section="Filter",
        option="parallelize",
        fallback=True
    )

    standardized_by_stmt_dir = configuration.config_parser.get(
        section="Standardizer",
        option="standardized_by_stmt_dir")

    concat_joined_by_stmt_dir = configuration.config_parser.get(
        section="Concat",
        option="concat_joined_by_stmt_dir")

    concat_joined_all_dir = configuration.config_parser.get(
        section="Concat",
        option="concat_joined_all_dir")

    concat_standardized_by_stmt_dir = configuration.config_parser.get(
        section="Concat",
        option="concat_standardized_by_stmt_dir")

    processes: List[AbstractProcess] = []

    processes.append(
        # 1. Filter, join, and save by stmt
        FilterProcess(db_dir=configuration.db_dir,
                      target_dir=filtered_joined_by_stmt_dir,
                      bag_type="joined",
                      save_by_stmt=True,
                      execute_serial=not filter_parallelize
                      )
    )

    processes.append(
        # 2. Standardize the data for every quarter
        StandardizeProcess(root_dir=f"{filtered_joined_by_stmt_dir}/quarter",
                           target_dir=standardized_by_stmt_dir),
    )

    processes.extend([
        # 3. building datasets with all entries by stmt
        ConcatByNewSubfoldersProcess(root_dir=f"{filtered_joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_joined_by_stmt_dir}/BS",
                                     pathfilter="*/BS"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{filtered_joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_joined_by_stmt_dir}/CF",
                                     pathfilter="*/CF"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{filtered_joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_joined_by_stmt_dir}/CI",
                                     pathfilter="*/CI"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{filtered_joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_joined_by_stmt_dir}/CP",
                                     pathfilter="*/CP"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{filtered_joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_joined_by_stmt_dir}/EQ",
                                     pathfilter="*/EQ"
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=f"{filtered_joined_by_stmt_dir}/quarter",
                                     target_dir=f"{concat_joined_by_stmt_dir}/IS",
                                     pathfilter="*/IS"
                                     )
    ])

    # 4. create a single joined bag with all the data filtered and joined
    processes.append(
        ConcatByChangedTimestampProcess(
            root_dir=concat_joined_by_stmt_dir,
            target_dir=concat_joined_all_dir,
        )
    )

    processes.extend([

        # 5. concate the standardized bags together by stmt (BS, IS, CF).
        ConcatByNewSubfoldersProcess(root_dir=standardized_by_stmt_dir,
                                     target_dir=f"{concat_standardized_by_stmt_dir}/BS",
                                     pathfilter="*/BS",
                                     in_memory=True # Standardized Bag only work with in_memory
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=standardized_by_stmt_dir,
                                     target_dir=f"{concat_standardized_by_stmt_dir}/CF",
                                     pathfilter="*/CF",
                                     in_memory=True # Standardized Bag only work with in_memory
                                     ),
        ConcatByNewSubfoldersProcess(root_dir=standardized_by_stmt_dir,
                                     target_dir=f"{concat_standardized_by_stmt_dir}/IS",
                                     pathfilter="*/IS",
                                     in_memory=True # Standardized Bag only work with in_memory
                                     )
    ])

    return processes
