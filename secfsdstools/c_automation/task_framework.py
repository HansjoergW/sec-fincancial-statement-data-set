"""
Base classes for the Task and Process Framework.
Used for downloading, transforming to parquet, and indexing of the zip files from SEC, as well
as to make additional automation tasks possible.
"""
import logging
import os
import shutil
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional
from typing import Protocol, Any, Dict

from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.a_utils.parallelexecution import ThreadExecutor


class TaskResultState(Enum):
    """
    Enum defining possible ResultStates of one task.
    """
    SUCCESS = 1
    FAILED = 2


class Task(Protocol):
    """
    Task interface.
    """

    def prepare(self):
        """ Prepare everything to execute the task.
            E.g., creation or clearing a directory. """

    def execute(self):
        """ Execution the task. """

    def commit(self) -> Any:
        """ Commit the task if the execution method is not "self-commiting". E.g.,
         If you do some file processing in the execute-method,
         but want to update a state in a table,
         you could do the update of the state in the commit method.
         """

    def exception(self, exception) -> Any:
        """ Handle the exception. """


@dataclass
class TaskResult:
    """
    Dataclass containing the result of a task.
    Contains the task, the TaskResultState and the result (either the return value form the commit()
    or exception() method.
    """
    task: Task
    result: Any
    state: TaskResultState


class BaseTask:
    """
    Implements the basic logic to track already processed data either by folder structure of the
    root-path (meaning that new data that appeared as a new subfolder in the root-path has to be
    integrated in the existing content of the target path) or by
    checking the timestamp of the latest modifications within the root-path structure (meaning
    that the content of the target-path has to be recreated with the current content of the
    root-path.

    Both scenarios use a "meta-inf" file that either contains the name of the subfolders, or the
    the timestamp of the latest processed modification.
    """

    def __init__(self,
                 root_path: Path,
                 filter: str,
                 target_path: Path,
                 check_by_timestamp: bool):
        """
        Constructor of base task.

        Args:
            root_path: root path to read that from
            filter: filter string that defines which subfolders in the root_path have to be selected
            target_path: path to where the results have to be written
            check_by_timestamp: defines whether selection for unprocessed data is being done
            by timestamp within the root_path or by subfolder names within the root_path.
        """

        self.root_path = root_path
        self.filter = filter
        self.check_by_timestamp = check_by_timestamp
        self.all_dirs = list(self.root_path.glob(self.filter))

        self.target_path = target_path

        self.tmp_path = target_path.parent / f"tmp_{target_path.name}"
        self.meta_inf_file: Path = self.target_path / "meta.inf"

        self.paths_to_process = self.all_dirs

        # filter could be something like "*", or "*/BS", or "something/*/BS"
        # but in order to be able to file the metainf file with the names for which "*" iterates
        # over, we need to know the position towards the end of the resulting path.
        # So if the filter is just a "*" it is 0, if it is "*/BS" it would be 1
        self.star_position = self._star_position_from_end(self.filter)

        # so if we have the filter */BS and if we have the directories "2010q1.zip/BS",
        # "2010q2.zip/BS" in the root_path, all_names key will be 2010q1.zip, 2010q2.zip
        self.all_names = {self._get_iterator_position_name(f, self.star_position):
                              f for f in self.all_dirs}

        if self.meta_inf_file.exists():
            containing_values = self.read_metainf_content()

            if self.check_by_timestamp:
                last_timestamp = float(containing_values[0])
                current_timestamp = get_latest_mtime(self.root_path)
                if current_timestamp <= last_timestamp:
                    self.paths_to_process = []
            else:
                missing = set(self.all_names.keys()) - set(containing_values)
                self.paths_to_process = [self.all_names[name] for name in missing]

    @staticmethod
    def _get_iterator_position_name(path: Path, star_position: int):
        return path.parts[::-1][star_position]

    @staticmethod
    def _star_position_from_end(path: str) -> int:
        # Split the string by '/' to get segments

        # ignore first and last /
        if path.startswith('/'):
            path = path[1:]
        if path.endswith('/'):
            path = path[:-1]

        segments = path.split('/')

        # Iterate from the end and find the first segment containing '*'
        for i, segment in enumerate(reversed(segments)):
            if '*' in segment:
                return i  # Position from the end

        # If no '*' is found, return -1 (or any error code that suits your needs)
        return -1

    def read_metainf_content(self) -> List[str]:
        """
        reads the content from the meta.inf file in an existing target_path
        Returns:
            List(str): the content as list, which is either a single timestamp or the name of
            subfolders in the target_path
        """
        meta_inf_content = self.meta_inf_file.read_text(encoding="utf-8")
        return meta_inf_content.split("\n")

    def prepare(self):
        """
        basic implementation of the prepare method. Does nothing if there is nothing to process
        or does create the tmp_folder, if processing has to be done.
        """
        if len(self.paths_to_process) == 0:
            return

        self.tmp_path.mkdir(parents=True, exist_ok=False)

    def commit(self):
        """
        Basic implementation of the commit method.
        If nothing had to be done, it simply returns "success".
        If work was done, it removes an existing target_path, and overwrites it with the
        content of the tmp_path (by renaming the tmp_path to the target_path, which is an
        atomic action, which either fails, or succeeds).
        """
        if len(self.paths_to_process) == 0:
            return "success"

        # Remove old content of target_path
        if self.target_path.exists():
            shutil.rmtree(self.target_path)

        # rename the tmp_path, so this is like an atomic action that either fails or succeeds.
        self.tmp_path.rename(self.target_path)
        return "success"

    def exception(self, exception) -> str:
        """
        Basic implementation of the exception method.
        It deletes the temp folder and returns a "failed" message.
        """
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        return f"failed {exception}"

    def execute(self):
        """
        Basic implementation of the execute method.

        If there are "paths_to_process", what has to be done depending on "check_by_timestamp"
        being true or not.


        Returns:

        """
        if len(self.paths_to_process) == 0:
            return

        paths_to_process = self.paths_to_process.copy()

        # If check_by_timestamp is false, then paths_to_process contain  names of subfolders
        # that have to be added to the data in an existing target_path.
        # Therefore, if the target_path exists...
        if self.target_path.exists() and not self.check_by_timestamp:
            # then we want to concate the existing data and the new data together and store
            # it as a new dataset in the target_path. So we add the current target_path
            # to the list of the folders, that have to be processed.
            paths_to_process.append(self.target_path)

        self.do_execution(paths_to_process)

        temp_meta_inf = self.tmp_path / "meta.inf"
        meta_inf_content: str
        if self.check_by_timestamp:
            meta_inf_content = str(get_latest_mtime(self.root_path))
        else:
            meta_inf_content = "\n".join([self._get_iterator_position_name(f, self.star_position)
                                          for f in self.paths_to_process])
        temp_meta_inf.write_text(data=meta_inf_content, encoding="utf-8")

    @abstractmethod
    def do_execution(self, paths_to_process: List[Path]):
        """
        handles the actual execution of the task
        """


class AbstractProcess(ABC):
    """
    Defines the Abstract process of processing tasks for a certain process.
    """

    def __init__(self,
                 execute_serial: bool = False,
                 chunksize: int = 3):
        self.execute_serial = execute_serial
        self.chunksize = chunksize

        # since failed tasks are retried, results[FAILED] can contain multiple entries for
        # a tasks that is retried multiple times.
        self.results: Dict[TaskResultState, List[TaskResult]] = defaultdict(list)

        self.failed_tasks: List[Task] = []

    @abstractmethod
    def calculate_tasks(self) -> List[Task]:
        """
        Calculate the tasks that have to be executed for the implemented process

        Returns:
            List[Tasks] : List of the tasks to be processed.
        """

    def pre_process(self):
        """ Hook method to implement logic that is executed before the whole process is finished. """

    def post_process(self):
        """ Hook method to implement logic that is executed after the whole process is finished. """

    @staticmethod
    def process_task(task: Task) -> TaskResult:
        """
        execute a single task.
        """
        logger = logging.getLogger()
        try:
            task.prepare()
            task.execute()
            result = TaskResult(task=task,
                                result=task.commit(),
                                state=TaskResultState.SUCCESS)
            logger.info("Success: %s", task)
            return result
        except Exception as ex:  # pylint: disable=W0703
            # we want to catch everything here.
            logger.info("Failed: %s / %s ", task, ex)
            return TaskResult(task=task,
                              result=task.exception(exception=ex),
                              state=TaskResultState.FAILED)

    def process(self):
        """
        execute the process by executing all the tasks that need to be executed.
        The execution can happen in parallel or serial.

        There is a retry mechanism for failing tasks.

        """
        logger = logging.getLogger()
        logger.info("Starting process %s", self.__class__.__name__)

        self.pre_process()

        executor = ThreadExecutor[Task, TaskResult, TaskResult](
            processes=3,
            max_calls_per_sec=8,
            chunksize=self.chunksize,
            execute_serial=self.execute_serial
        )
        executor.set_get_entries_function(self.calculate_tasks)
        executor.set_process_element_function(self.process_task)
        executor.set_post_process_chunk_function(lambda x: x)  # no process_chunk for this purpose

        results_all, self.failed_tasks = executor.execute()

        for entry in results_all:
            self.results[entry.state].append(entry)

        for failed in self.failed_tasks:
            logger.warning("not able to process %s", failed)

        self.post_process()


def execute_processes(processes: List[AbstractProcess]):
    """
        Execute the list of processes in serial
    Args:
        processes (List(AbstractProcess)): List of AbstractProcesses to be executed

    """
    for process in processes:
        process.process()


def delete_temp_folders(root_path: Path, temp_prefix: str = "tmp"):
    """
    Remove any existing folders starting with the tmp_prefix in the root_path
     (these are generally folders containing data of tasks that failed)).

    """
    dirs_in_filter_dir = get_directories_in_directory(str(root_path))

    tmp_dirs = [d for d in dirs_in_filter_dir if d.startswith(temp_prefix)]

    for tmp_dir in tmp_dirs:
        file_path = root_path / tmp_dir
        shutil.rmtree(file_path, ignore_errors=True)


def get_latest_mtime(folder: Path, skip: Optional[List[str]] = None) -> float:
    """
    Find the latest timestamp at which an element in the folder structure was changed
    Args:
        folder: root folder

    Returns:
        the latest timestamp of a folder or file within the "folder" as float value.

    """
    if skip == None:
        skip = []

    latest_mtime = 0

    for dirpath, dirnames, filenames in os.walk(folder):
        # Check the modification timestamp of files
        filenames = list(set(filenames) - set(skip))
        for filename in filenames:
            file_path = Path(dirpath) / filename
            mtime = file_path.stat().st_mtime  # modification timestamp of the file
            latest_mtime = max(latest_mtime, mtime)

        # Check the modification timestamp of subfolders
        for dirname in dirnames:
            dir_path = Path(dirpath) / dirname
            mtime = dir_path.stat().st_mtime  # modification timestamp of the folder
            latest_mtime = max(latest_mtime, mtime)

    return latest_mtime
