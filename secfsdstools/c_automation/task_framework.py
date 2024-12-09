"""
Base classes for the Task and Process Framework.
Used for downloading, transforming to parquet, and indexing of the zip files from SEC, as well
as to implement customized automation tasks.
"""
import logging
import shutil
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Set, Tuple
from typing import Protocol, Any, Dict

from secfsdstools.a_utils.parallelexecution import ThreadExecutor, ParallelExecutor
from secfsdstools.c_automation.automation_utils import get_latest_mtime


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


class AbstractTask:
    """
    Abstract Base implemenation providing some commonly used basic functionality.

    It is based on reading subfolders from a root_path, which are defined by filter.
    Then processing the content of these folders and writing the result in a target_path.

    The result is created in tmp-folder and is then "commited" by renaming the tmp-folder into
    the target-path, therefore providing an atomic-action (renaming) that acts as commit.

    It also provides basic implementation of "meta.inf" file, that can be stored in the target.
    The idea of the meta.inf file is, to give a hint of what already was processed from the
    root_path in a previous, step.

    For example, the meta.inf could contain a list of subfolder names that were already processed.
    Therefore, if a new subfolder appears in the root_path, the task would knwow which subfolders
    need to be process. another possibility is to store the timestamp of the data, which was
    processed (in cases, where the content of files within the subfolders in root_path changes, but
    not the subfolders themselves). Therefore, allowing to check whether a modification timestamp
    of files in the root_path is newer than the timestamp stored in the meta.inf file.

    """

    def __init__(self,
                 root_path: Path,
                 filter: str,
                 target_path: Path):
        """
        The constructor of the AbstracTask.

        Args:
            root_path: root_path of the data to be processed
            filter: a filter string (e.g. "*"; as defined for Path.glob()) to select the
                    subfolders, that have to be processed.
                    filter could be something like "*", or "*/BS", or "something/*/BS".

                    E.g., the following root_path structure and the filter "*/BS"
                    would select all "BS" "sub-subfolders" within root_path:
                    <pre>
                       <root_path>
                            2010a1.zip/BS
                            2010q1.zip/IS
                            2010q1.zip/CF
                            ...
                            2024a4.zip/BS
                            2024q4.zip/IS
                            2024q4.zip/CF
                    </pre>

            target_path: the target_path to write the results to.
        """

        self.root_path = root_path
        self.target_path = target_path
        self.filter = filter

        # create a list of subfolders that have to be processed defined by the filter string.
        self.filtered_paths = list(self.root_path.glob(self.filter))

        # usually, all filtered_paths have to be processed
        self.paths_to_process: List[Path] = self.filtered_paths

        # define the tmp_path
        self.tmp_path = target_path.parent / f"tmp_{target_path.name}"
        self.meta_inf_file: Path = self.target_path / "meta.inf"

        # filter could be something like "*", or "*/BS", or "something/*/BS"
        # but in order to be able to fill the metainf file with the names for which "*" iterates
        # over, we need to know the position of the "*" from the end of the resulting path.
        # So if the filter is just a "*" it is 0, if it is "*/BS" it would be 1
        self.star_position = self._star_position_from_end(self.filter)

    @staticmethod
    def _star_position_from_end(path: str) -> int:
        """
        Gets the position of the "*" in the provided path (counted from the end).

        Examples:
            path = "a/b/c/d/*" -> returns 0
            path = "a/b/c/*/d" -> returns 1
            path = "a/b/*/c/d" -> returns 2

        Args:
            path: path with a "*" as part

        Returns:
            the position of the "*" in the path, counted from the end.
        """

        # ignore first and last /
        if path.startswith('/'):
            path = path[1:]
        if path.endswith('/'):
            path = path[:-1]

        # Split the string by '/' to get segments
        segments = path.split('/')

        # Iterate from the end and find the first segment containing '*'
        for i, segment in enumerate(reversed(segments)):
            if '*' in segment:
                return i  # Position from the end

        # If no '*' is found, return -1 to indicate an error
        return -1

    @staticmethod
    def _get_star_position_name(path: Path, star_position: int) -> str:
        """
        Gets the name of the part where the "*" is positioned in the filter-string.

        Example:
             path = "a/b/c" and star_position = 0 -> returns c
             path = "a/b/c" and star_position = 1 -> returns b
             path = "a/b/c" and star_position = 2 -> returns c

        Args:
            path: path from which the name_part at the star_position has to be returned
            star_position: position of the part which name has to be returned.

        Returns:
            str: name of the part defined by the star_position

        """
        # reverse list with [::-1]
        return path.parts[::-1][star_position]

    def read_metainf_content(self) -> List[str]:
        """
        reads the content from the meta.inf file in an existing target_path
        Returns:
            List(str): the content by line
        """
        meta_inf_content = self.meta_inf_file.read_text(encoding="utf-8")
        return meta_inf_content.split("\n")

    def exception(self, exception) -> str:
        """
        Basic implementation of the exception method.
        It deletes the temp folder and returns a "failed" message.
        """
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        return f"failed {exception}"

    def has_work_todo(self) -> bool:
        """
        returns true if there is actual work to do, otherwise False.
        Can be overwritten.
        Default implementation just looks if the provided root_path has subfolders, that are
        defined by the provided filter string.
        """
        return len(self.paths_to_process) > 0

    def prepare(self):
        """
        basic implementation of the prepare method. Does nothing if there is nothing to process
        or does create the tmp_folder, if processing has to be done.
        """
        if not self.has_work_todo():
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
        if not self.has_work_todo():
            return "success"

        # Remove old content of target_path
        if self.target_path.exists():
            shutil.rmtree(self.target_path)

        # rename the tmp_path, so this is like an atomic action that either fails or succeeds.
        self.tmp_path.rename(self.target_path)
        return "success"

    def write_meta_inf(self, content: str):
        temp_meta_inf = self.tmp_path / "meta.inf"
        temp_meta_inf.write_text(data=content, encoding="utf-8")


class CheckByTimestampMergeBaseTask(AbstractTask):
    """
    This class uses the AbstractTask to implement logic that checks if files were changed within
    the root_path since the last processing.

    It can be used as a BaseClass to implement a Task, that checks for new data to be processed
    by looking at the modification timestamp of the files in the root_path.

    It does this as follows:
    - if there is no target_path yet, it will process the content in the root_path,
      write the result in the target_path together with a meta.inf file that contains
      the newest modification timestamp of all the files in the root_path.
    - if there is a target_path, then it reads the timestamp, that is stored within the target_path.
      It any of the files within the root_path has a newer modification timestamp, it will
      process the data and also update the timestamp in the meta.inf file
    """

    def __init__(self,
                 root_path: Path,
                 filter: str,
                 target_path: Path):
        """
          The constructor of the CheckByTimestampMergeBaseTask.
          Check also the documentation of the AbstractTask Constructor.
        """
        super().__init__(
            root_path=root_path,
            filter=filter,
            target_path=target_path,
        )

        if self.meta_inf_file.exists():
            # if the meta_inf file exists, we expect that the first row contains the
            # latest modification timestamp of all files in the root_path, that was
            # processed the last time.
            containing_values = self.read_metainf_content()
            last_processed_timestamp = float(containing_values[0])

            # go and find the current latest modification timestamp of allfiles in the root_path
            current_timestamp = get_latest_mtime(self.root_path)

            # if the current_timestamp is equal to the last_processed_timestamp,
            # it means that the data in the root_path weren't changed and therefore,
            # no processing has to be done. We mark this by setting pats_to_process to an empty list
            if current_timestamp <= last_processed_timestamp:
                self.paths_to_process = []

    def execute(self):
        """
        Basic implementation of the execute method.

        If there are "paths_to_process", what has to be done depending on "check_by_timestamp"
        being true or not.

        Returns:

        """
        if not self.has_work_todo():
            return

        self.do_execution(paths_to_process=self.paths_to_process,
                          tmp_path=self.tmp_path)

        meta_inf_content: str = str(get_latest_mtime(self.root_path))
        self.write_meta_inf(content=meta_inf_content)

    @abstractmethod
    def do_execution(self, paths_to_process: List[Path],
                     tmp_path: Path):
        """
            defines the logic to be executed.
        Args:
            paths_to_process: lists of paths/folders that have to be processec
            tmp_path: path to where a result has to be written
        """


class CheckByNewSubfoldersMergeBaseTask(AbstractTask):
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
                 target_path: Path):
        """
        Constructor of base task.

        Args:
            root_path: root path to read that from
            filter: filter string that defines which subfolders in the root_path have to be selected
            target_path: path to where the results have to be written
            check_by_timestamp: defines whether selection for unprocessed data is being done
            by timestamp within the root_path or by subfolder names within the root_path.
        """
        self.all_names: Dict[str, Path]

        super().__init__(
            root_path=root_path,
            filter=filter,
            target_path=target_path,
        )

        # so if we have the filter */BS and if we have the directories "2010q1.zip/BS",
        # "2010q2.zip/BS" in the root_path, all_names key will be 2010q1.zip, 2010q2.zip
        self.all_names = {self._get_star_position_name(path=p, star_position=self.star_position):
                              p for p in self.paths_to_process}

        if self.meta_inf_file.exists():
            containing_values = self.read_metainf_content()

            missing = set(self.all_names.keys()) - set(containing_values)
            self.paths_to_process = [self.all_names[name] for name in missing]

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

        # depending on the use case, we need to combine new data with already
        # existing data in the target.
        # therefore, we provide a list "paths_to_process" which contains subfolders that are new,
        # the processed_path (the path that contains the result of the last processing), and
        # the target_path, where we have to store the result to (this the tmp folder)
        self.do_execution(paths_to_process=paths_to_process,
                          target_path=self.target_path,
                          tmp_path=self.tmp_path)

        meta_inf_content: str = "\n".join([self._get_star_position_name(f, self.star_position)
                                           for f in self.filtered_paths])
        self.write_meta_inf(content=meta_inf_content)

    @abstractmethod
    def do_execution(self,
                     paths_to_process: List[Path],
                     target_path: Path,
                     tmp_path: Path):
        """
            defines the logic to be executed.
        Args:
            paths_to_process: lists of paths/folders that have to be processed
            target_path: the path where the result of the previous run was written
            tmp_path: target path to where a result has to be written
        """


class AbstractProcess(ABC):
    """
    Defines the Abstract process of processing tasks for a certain process.
    """

    def __init__(self,
                 execute_serial: bool = False,
                 chunksize: int = 3,
                 paralleltasks: int = 3,
                 max_tasks_per_second: int = 8):
        self.execute_serial = execute_serial
        self.chunksize = chunksize
        self.paralleltasks = paralleltasks
        self.max_tasks_per_second = max_tasks_per_second

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

        results_all, failed_tasks = self.do_execution()
        self.failed_tasks = failed_tasks

        for entry in results_all:
            self.results[entry.state].append(entry)

        for failed in self.failed_tasks:
            logger.warning("not able to process %s", failed)

        self.post_process()

    @abstractmethod
    def do_execution(self) -> Tuple[List[TaskResult], List[Task]]:
        """
        handle to real execution.
        """


class AbstractThreadProcess(AbstractProcess):
    """
    Uses for the parallel execution logic a Thread-Based approach.
    """

    def do_execution(self) -> Tuple[List[TaskResult], List[Task]]:
        """
        Using a thread-based executor.
        """
        executor = ThreadExecutor[Task, TaskResult, TaskResult](
            processes=self.paralleltasks,
            max_calls_per_sec=self.max_tasks_per_second,
            chunksize=self.chunksize,
            execute_serial=self.execute_serial
        )
        executor.set_get_entries_function(self.calculate_tasks)
        executor.set_process_element_function(self.process_task)
        executor.set_post_process_chunk_function(lambda x: x)  # no process_chunk for this purpose
        return executor.execute()


class AbstractProcessPoolProcess(AbstractProcess):
    """
    Uses for the parallel execution logic a Thread-Based approach.
    """

    def do_execution(self) -> Tuple[List[TaskResult], List[Task]]:
        """
        Using a process-based executor.
        """
        executor = ParallelExecutor[Task, TaskResult, TaskResult](
            processes=self.paralleltasks,
            max_calls_per_sec=self.max_tasks_per_second,
            chunksize=self.chunksize,
            execute_serial=self.execute_serial
        )
        executor.set_get_entries_function(self.calculate_tasks)
        executor.set_process_element_function(self.process_task)
        executor.set_post_process_chunk_function(lambda x: x)  # no process_chunk for this purpose
        return executor.execute()


def execute_processes(processes: List[AbstractProcess]):
    """
        Execute the list of processes in serial
    Args:
        processes (List(AbstractProcess)): List of AbstractProcesses to be executed

    """
    for process in processes:
        process.process()
