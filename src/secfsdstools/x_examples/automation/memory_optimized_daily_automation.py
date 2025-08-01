# pylint: disable=C0301
"""
This module shows the automation to add additional steps after the usual update process
(which is downloading new zip files, transforming them to parquet, indexding them).

This example has a low memory footprint, so it is safe to use also on systems with 16GB
of memory. It also activates daily processing.

If you update the configuration as defined below, you will get the following datasets:

- for quarterly data provided and parsed by the SEC:
    - single filtered and joined bags for every stmt (BS, IS, CF, ..) containing the data from
      all available zip files.
    - a single filtered and joined bag containing the data from all available zip files.
    - single standardized bags for BS, IS, CF containing the data from all available zip files.

- for daily data provided by the SEC and parsed locally with the `secdaily` package
  (not yet available as quaterly data):
    - single filtered and joined bags for every stmt (BS, IS, CF, ..) containing daily parsed data.
    - a single filtered and joined bag containing the data from all daily parsed data.
    - single standardized bags for BS, IS, CF containing the data from all daily parsed data.

- combined quarterly and daily data:
    - single filtered and joined bags for every stmt (BS, IS, CF, ..) from all available data
    - a single filtered and joined bag containing the data from all available data.
    - single standardized bags for BS, IS, CF containing the data from all available data.


Moreover, these files will be automatically updated as soon as a new quarterly zip file or a new daily data
becomes available at the SEC website.

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

postupdateprocesses=secfsdstools.x_examples.automation.memory_optimized_daily_automation.define_extra_processes

# activate daily processing
dailyprocessing = True

</pre>

If you want to use it, you also need to add additional configuration entries as shown below:

<pre>
[Filter]
filtered_quarterly_joined_by_stmt_dir = C:/data/sec/automated/_1_by_quarter/_1_filtered_joined_by_stmt
filtered_daily_joined_by_stmt_dir = C:/data/sec/automated/_1_by_day/_1_filtered_joined_by_stmt
parallelize = True

[Standardizer]
standardized_quarterly_by_stmt_dir = C:/data/sec/automated/_1_by_quarter/_2_standardized_by_stmt
standardized_daily_by_stmt_dir = C:/data/sec/automated/_1_by_day/_2_standardized_by_stmt

[Concat]
concat_quarterly_joined_by_stmt_dir = C:/data/sec/automated/_2_all_quarter/_1_joined_by_stmt
concat_daily_joined_by_stmt_dir = C:/data/sec/automated/_2_all_day/_1_joined_by_stmt

concat_quarterly_joined_all_dir = C:/data/sec/automated/_2_all_quarter/_2_joined
concat_daily_joined_all_dir = C:/data/sec/automated/_2_all_day/_2_joined

concat_quarterly_standardized_by_stmt_dir = C:/data/sec/automated/_2_all_quarter/_3_standardized_by_stmt
concat_daily_standardized_by_stmt_dir = C:/data/sec/automated/_2_all_day/_3_standardized_by_stmt

concat_all_joined_by_stmt_dir = C:/data/sec/automated/_3_all/_1_joined_by_stmt
concat_all_joined_dir = C:/data/sec/automated/_3_all/_2_joined
concat_all_standardized_by_stmt_dir = C:/data/sec/automated/_3_all/_3_standardized_by_stmt

</pre>

(A complete configuration file using the "define_extra_processes" function is available in the file
 memory_optimized_daily_automation_config.cfg which is in the same package as this module here.)

This example adds the following main steps to the usual update process.

- for quarterly data provided and parsed by the SEC, as well as daily data not yet part of the quarterly data:

    First, it creates a joined bag for every zip file, filters it for 10-K and 10-Q reports only
    and also applies the filters  ReportPeriodRawFilter, MainCoregRawFilter, USDOnlyRawFilter,
    OfficialTagsOnlyRawFilter. Furthermore, the data will also be split by stmt.
    Location: filtered_[quarter/daily]_joined_by_stmt_dir.
    Note: setting "parallelize" in the config to False, will be slower in the initial loading
    but using less memory.

    Second, it produces standardized bags for BS, IS, CF for every zip file based on the filtered
    data from the previous step. These bags are stored under the path defined as
    `standardized_[quarterly/daily]_by_stmt_dir`.

    Third, it creates a single joined bag for every statement (balance sheet, income statement,
    cash flow, ...) based on the filtered data from the first step.
    These bags are stored under the path defined as `concat_[quarterly/daily]_joined_by_stmt_dir`.

    Fourth, it will create a single bag with all data from all different statements, by merging the
    bags from the previous step into a single big bag. This bag will be stored under the path defined
    as `concat_[quarterly,daily]_joined_all_dir`.

    Fifth, it will create single standardized bags for BS, IS, CF from the results in the second step.
    These bags will be stored under the path defined as `concat_[quarterly/daily]_standardized_by_stmt_dir`.

- combined quarterly and daily data:

    First, it creates a single joined bag for every statement (balance sheet, income statement,
    cash flow, ...) containing quarterly and daily data.
    These bags are stored under the path defined as `concat_all_joined_by_stmt_dir`.

    Second, it will create a single bag with all data from all different statements, by merging the
    bags from the previous step into a single big bag. This bag will be stored under the path defined
    as `concat_all_joined_all_dir`.

    Third, it will create single standardized bags for BS, IS, CF containing data from both quarterly and daily data.
    These bags will be stored under the path defined as `concat_all_standardized_by_stmt_dir`.


All this steps use basic implementations of the AbstractProcess class from the
secfsdstools.g_pipeline package.

Furthermore, all these steps check if something changed since the last run and are only executed
if something did change (for instance, if a new data became available).

Have also a look at the notebook 08_00_automation_basics.

"""
import shutil
from pathlib import Path
from typing import List

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_automation.task_framework import AbstractProcess, LoggingProcess
from secfsdstools.c_daily.dailypreparation_process import DailyPreparationProcess
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
from secfsdstools.g_pipelines.concat_process import (
    ConcatByChangedTimestampProcess,
    ConcatByNewSubfoldersProcess,
    ConcatMultiRootByChangedTimestampProcess,
)
from secfsdstools.g_pipelines.filter_process import FilterProcess
from secfsdstools.g_pipelines.standardize_process import StandardizeProcess


class ClearDailyDataProcess(AbstractProcess):
    """
    Since daily processed data is removed, as soon as the data is contained in a quarterly zip file,
    we also need to remove the according data from the automated datasets.

    This process does this for the filtered and standardized daily data, which are created in the first step
    of processing the daily data.

    The following steps for the daily processing always creates the dataset fresh from all available daily data.
    Hence, we only need to clean the filtered and standardized daily data.
    """

    def __init__(self, db_dir: str, filtered_daily_joined_by_stmt_dir: str, standardized_daily_by_stmt_dir: str):
        """
        Constructor.
        Args:
            db_dir: directory of the database
            filtered_daily_joined_by_stmt_dir: directory containing the filtered daily data
            standardized_daily_by_stmt_dir: directory containing the standardized daily data
        """
        super().__init__()
        self.db_dir = db_dir
        self.index_accessor = ParquetDBIndexingAccessor(db_dir=db_dir)
        self.filtered_daily_joined_by_stmt_dir = filtered_daily_joined_by_stmt_dir
        self.standardized_daily_by_stmt_dir = standardized_daily_by_stmt_dir

    def clear_directory(self, cut_off_day: int, root_dir_path: Path):
        """
        removes all directories under root_dir_path that are older than the cut_off_day.
        """

        cut_off_file_name = f"{cut_off_day}.zip"

        if root_dir_path.exists():
            for dir_path in root_dir_path.iterdir():
                if dir_path.is_dir() and dir_path.name < cut_off_file_name:
                    shutil.rmtree(dir_path)

    def process(self):
        """
        Execute the process.
        """
        last_processed_quarter_file_name = self.index_accessor.find_latest_quarter_file_name()
        if last_processed_quarter_file_name is None:
            raise ValueError(
                "No quarterly files were processed before. "
                "Please process quarterly files first before running the daily process."
            )
        last_processed_quarter = last_processed_quarter_file_name.split(".")[0]

        daily_start_quarter = DailyPreparationProcess.calculate_daily_start_quarter(last_processed_quarter)
        cut_off_day = DailyPreparationProcess.cut_off_day(daily_start_quarter)
        self.clear_directory(
            cut_off_day=cut_off_day, root_dir_path=Path(self.filtered_daily_joined_by_stmt_dir) / "daily"
        )
        self.clear_directory(cut_off_day=cut_off_day, root_dir_path=Path(self.standardized_daily_by_stmt_dir))


# pylint: disable=too-many-locals
def define_extra_processes(configuration: Configuration) -> List[AbstractProcess]:
    """
    example definition of an additional pipeline. It adds sevreal steps to process
    quarterly and daily data, as well as combining both. See the documentation of this
    module for more details.

    All these steps have a low memory footprint, so, the should run without any problems also
    on 16 GB machine.

    Please have a look at the notebook 08_00_automation_basics for further details.

    Args:
        configuration: the configuration

    Returns:
        List[AbstractProcess]: List with the defined process steps

    """

    filtered_quarterly_joined_by_stmt_dir = configuration.get_parser().get(
        section="Filter", option="filtered_quarterly_joined_by_stmt_dir"
    )
    filtered_daily_joined_by_stmt_dir = configuration.get_parser().get(
        section="Filter", option="filtered_daily_joined_by_stmt_dir"
    )

    filter_parallelize = configuration.get_parser().get(section="Filter", option="parallelize", fallback=True)

    standardized_quarterly_by_stmt_dir = configuration.get_parser().get(
        section="Standardizer", option="standardized_quarterly_by_stmt_dir"
    )
    standardized_daily_by_stmt_dir = configuration.get_parser().get(
        section="Standardizer", option="standardized_daily_by_stmt_dir"
    )

    concat_quarterly_joined_by_stmt_dir = configuration.get_parser().get(
        section="Concat", option="concat_quarterly_joined_by_stmt_dir"
    )
    concat_daily_joined_by_stmt_dir = configuration.get_parser().get(
        section="Concat", option="concat_daily_joined_by_stmt_dir"
    )

    concat_quarterly_joined_all_dir = configuration.get_parser().get(
        section="Concat", option="concat_quarterly_joined_all_dir"
    )
    concat_daily_joined_all_dir = configuration.get_parser().get(section="Concat", option="concat_daily_joined_all_dir")

    concat_quarterly_standardized_by_stmt_dir = configuration.get_parser().get(
        section="Concat", option="concat_quarterly_standardized_by_stmt_dir"
    )
    concat_daily_standardized_by_stmt_dir = configuration.get_parser().get(
        section="Concat", option="concat_daily_standardized_by_stmt_dir"
    )

    concat_all_joined_by_stmt_dir = configuration.get_parser().get(
        section="Concat", option="concat_all_joined_by_stmt_dir"
    )

    concat_all_joined_dir = configuration.get_parser().get(section="Concat", option="concat_all_joined_dir")

    concat_all_standardized_by_stmt_dir = configuration.get_parser().get(
        section="Concat", option="concat_all_standardized_by_stmt_dir"
    )

    processes: List[AbstractProcess] = []

    # QUARTERLY DATA Processing
    processes.append(LoggingProcess(title="Post Update Processes For Quarterly Data Started", lines=[]))

    processes.append(
        # 1. Filter, join, and save by stmt
        FilterProcess(
            db_dir=configuration.db_dir,
            target_dir=filtered_quarterly_joined_by_stmt_dir,
            bag_type="joined",
            save_by_stmt=True,
            execute_serial=not filter_parallelize,
            file_type="quarter",
        )
    )

    processes.append(
        # 2. Standardize the data for every quarter
        StandardizeProcess(
            root_dir=f"{filtered_quarterly_joined_by_stmt_dir}/quarter", target_dir=standardized_quarterly_by_stmt_dir
        ),
    )

    processes.extend(
        [
            # 3. building datasets with all entries by stmt
            ConcatByNewSubfoldersProcess(
                root_dir=f"{filtered_quarterly_joined_by_stmt_dir}/quarter",
                target_dir=f"{concat_quarterly_joined_by_stmt_dir}/BS",
                pathfilter="*/BS",
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=f"{filtered_quarterly_joined_by_stmt_dir}/quarter",
                target_dir=f"{concat_quarterly_joined_by_stmt_dir}/CF",
                pathfilter="*/CF",
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=f"{filtered_quarterly_joined_by_stmt_dir}/quarter",
                target_dir=f"{concat_quarterly_joined_by_stmt_dir}/CI",
                pathfilter="*/CI",
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=f"{filtered_quarterly_joined_by_stmt_dir}/quarter",
                target_dir=f"{concat_quarterly_joined_by_stmt_dir}/CP",
                pathfilter="*/CP",
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=f"{filtered_quarterly_joined_by_stmt_dir}/quarter",
                target_dir=f"{concat_quarterly_joined_by_stmt_dir}/EQ",
                pathfilter="*/EQ",
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=f"{filtered_quarterly_joined_by_stmt_dir}/quarter",
                target_dir=f"{concat_quarterly_joined_by_stmt_dir}/IS",
                pathfilter="*/IS",
            ),
        ]
    )

    # 4. create a single joined bag with all the data filtered and joined
    processes.append(
        ConcatByChangedTimestampProcess(
            root_dir=concat_quarterly_joined_by_stmt_dir,
            target_dir=concat_quarterly_joined_all_dir,
        )
    )

    # 5. concate the standardized bags together by stmt (BS, IS, CF).
    processes.extend(
        [
            ConcatByNewSubfoldersProcess(
                root_dir=standardized_quarterly_by_stmt_dir,
                target_dir=f"{concat_quarterly_standardized_by_stmt_dir}/BS",
                pathfilter="*/BS",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=standardized_quarterly_by_stmt_dir,
                target_dir=f"{concat_quarterly_standardized_by_stmt_dir}/CF",
                pathfilter="*/CF",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=standardized_quarterly_by_stmt_dir,
                target_dir=f"{concat_quarterly_standardized_by_stmt_dir}/IS",
                pathfilter="*/IS",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
        ]
    )

    # DAILY DATA Processing
    processes.append(LoggingProcess(title="Post Update Processes For Daily Data Started", lines=[]))

    # clean daily data covered now by quarterly data
    processes.append(
        ClearDailyDataProcess(
            db_dir=configuration.db_dir,
            filtered_daily_joined_by_stmt_dir=filtered_daily_joined_by_stmt_dir,
            standardized_daily_by_stmt_dir=standardized_daily_by_stmt_dir,
        )
    )

    # 1. Filter, join, and save by stmt
    processes.append(
        FilterProcess(
            db_dir=configuration.db_dir,
            target_dir=filtered_daily_joined_by_stmt_dir,
            bag_type="joined",
            save_by_stmt=True,
            execute_serial=not filter_parallelize,
            file_type="daily",
        )
    )

    processes.append(
        # 2. Standardize the data for daily data
        StandardizeProcess(
            root_dir=f"{filtered_daily_joined_by_stmt_dir}/daily", target_dir=standardized_daily_by_stmt_dir
        ),
    )

    processes.extend(
        [
            # 3. building datasets with all entries by stmt for daily data
            ConcatByChangedTimestampProcess(
                root_dir=f"{filtered_daily_joined_by_stmt_dir}/daily",
                target_dir=f"{concat_daily_joined_by_stmt_dir}/BS",
                pathfilter="*/BS",
            ),
            ConcatByChangedTimestampProcess(
                root_dir=f"{filtered_daily_joined_by_stmt_dir}/daily",
                target_dir=f"{concat_daily_joined_by_stmt_dir}/CF",
                pathfilter="*/CF",
            ),
            ConcatByChangedTimestampProcess(
                root_dir=f"{filtered_daily_joined_by_stmt_dir}/daily",
                target_dir=f"{concat_daily_joined_by_stmt_dir}/CI",
                pathfilter="*/CI",
            ),
            ConcatByChangedTimestampProcess(
                root_dir=f"{filtered_daily_joined_by_stmt_dir}/daily",
                target_dir=f"{concat_daily_joined_by_stmt_dir}/CP",
                pathfilter="*/CP",
            ),
            ConcatByChangedTimestampProcess(
                root_dir=f"{filtered_daily_joined_by_stmt_dir}/daily",
                target_dir=f"{concat_daily_joined_by_stmt_dir}/EQ",
                pathfilter="*/EQ",
            ),
            ConcatByChangedTimestampProcess(
                root_dir=f"{filtered_daily_joined_by_stmt_dir}/daily",
                target_dir=f"{concat_daily_joined_by_stmt_dir}/IS",
                pathfilter="*/IS",
            ),
        ]
    )

    # 4. create a single joined bag with all the data filtered and joined for daily data
    processes.append(
        ConcatByChangedTimestampProcess(
            root_dir=concat_daily_joined_by_stmt_dir,
            target_dir=concat_daily_joined_all_dir,
        )
    )

    # 5. concate the standardized bags together by stmt (BS, IS, CF) for daily data.
    processes.extend(
        [
            ConcatByNewSubfoldersProcess(
                root_dir=standardized_daily_by_stmt_dir,
                target_dir=f"{concat_daily_standardized_by_stmt_dir}/BS",
                pathfilter="*/BS",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=standardized_daily_by_stmt_dir,
                target_dir=f"{concat_daily_standardized_by_stmt_dir}/CF",
                pathfilter="*/CF",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
            ConcatByNewSubfoldersProcess(
                root_dir=standardized_daily_by_stmt_dir,
                target_dir=f"{concat_daily_standardized_by_stmt_dir}/IS",
                pathfilter="*/IS",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
        ]
    )

    # Concat daily and quarter together
    processes.append(
        LoggingProcess(title="Post Update Processes To Combine Quarterly And Daily Data Started", lines=[])
    )

    # 1. concat joined_by_statement
    processes.extend(
        [
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_quarterly_joined_by_stmt_dir, concat_daily_joined_by_stmt_dir],
                target_dir=f"{concat_all_joined_by_stmt_dir}/BS",
                pathfilter="BS",
            ),
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_quarterly_joined_by_stmt_dir, concat_daily_joined_by_stmt_dir],
                target_dir=f"{concat_all_joined_by_stmt_dir}/CF",
                pathfilter="CF",
            ),
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_quarterly_joined_by_stmt_dir, concat_daily_joined_by_stmt_dir],
                target_dir=f"{concat_all_joined_by_stmt_dir}/CI",
                pathfilter="CI",
            ),
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_quarterly_joined_by_stmt_dir, concat_daily_joined_by_stmt_dir],
                target_dir=f"{concat_all_joined_by_stmt_dir}/CP",
                pathfilter="CP",
            ),
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_quarterly_joined_by_stmt_dir, concat_daily_joined_by_stmt_dir],
                target_dir=f"{concat_all_joined_by_stmt_dir}/EQ",
                pathfilter="EQ",
            ),
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_quarterly_joined_by_stmt_dir, concat_daily_joined_by_stmt_dir],
                target_dir=f"{concat_all_joined_by_stmt_dir}/IS",
                pathfilter="IS",
            ),
        ]
    )

    # 2. concat joined
    processes.append(
        ConcatMultiRootByChangedTimestampProcess(
            root_dirs=[concat_daily_joined_all_dir, concat_quarterly_joined_all_dir],
            pathfilter="",
            target_dir=concat_all_joined_dir,
        )
    )

    # 3. concat standardized by statement
    processes.extend(
        [
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_daily_standardized_by_stmt_dir, concat_quarterly_standardized_by_stmt_dir],
                target_dir=f"{concat_all_standardized_by_stmt_dir}/BS",
                pathfilter="BS",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_daily_standardized_by_stmt_dir, concat_quarterly_standardized_by_stmt_dir],
                target_dir=f"{concat_all_standardized_by_stmt_dir}/CF",
                pathfilter="CF",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
            ConcatMultiRootByChangedTimestampProcess(
                root_dirs=[concat_daily_standardized_by_stmt_dir, concat_quarterly_standardized_by_stmt_dir],
                target_dir=f"{concat_all_standardized_by_stmt_dir}/IS",
                pathfilter="IS",
                in_memory=True,  # Standardized Bag only work with in_memory
            ),
        ]
    )
    return processes
