"""
Base classes for the Task and Process Framework.
"""
import os
import logging
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
        # a continuing failing task
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

def delete_temp_folders(root_path: Path, temp_prefix: str = "tmp"):
    """
    remove any existing folders starting with the tmp_prefix (folders that were not successfully completed

    """
    dirs_in_filter_dir = get_directories_in_directory(str(root_path))

    tmp_dirs = [d for d in dirs_in_filter_dir if d.startswith(temp_prefix)]

    for tmp_dir in tmp_dirs:
        file_path = root_path / tmp_dir
        shutil.rmtree(file_path, ignore_errors=True)


def get_latest_mtime(folder: Path, skip: Optional[List[str]] = None) -> float:
    """
    find the latest timestamp at which an element in the folder structure was changed
    Args:
        folder: root folder

    Returns:

    """
    if skip == None:
        skip = []

    latest_mtime = 0

    for dirpath, dirnames, filenames in os.walk(folder):
        # Prüfe alle Dateien im aktuellen Verzeichnis
        filenames = list(set(filenames) - set(skip))
        for filename in filenames:
            file_path = Path(dirpath) / filename
            mtime = file_path.stat().st_mtime  # Änderungszeitpunkt der Datei
            latest_mtime = max(latest_mtime, mtime)

        # Prüfe die Änderungszeitpunkte der Unterverzeichnisse
        for dirname in dirnames:
            dir_path = Path(dirpath) / dirname
            mtime = dir_path.stat().st_mtime  # Änderungszeitpunkt des Verzeichnisses
            latest_mtime = max(latest_mtime, mtime)

    return latest_mtime