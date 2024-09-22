import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, List, Any, Dict

from secfsdstools.a_utils.parallelexecution import ThreadExecutor


class TaskResultState(Enum):
    SUCCESS = 1
    FAILED = 2


class Task(Protocol):
    def prepare(self):
        """ Prepare everything to execute the task.
            E.g., creation or clearing a directory. """

    def execute(self):
        """ Execution the task. """

    def commit(self) -> str:
        """ Commit the task. """

    def exception(self, exception) -> str:
        """ Report the exception. """


@dataclass
class TaskResult:
    task: Task
    result: Any
    state: TaskResultState


class AbstractProcess(ABC):

    def __init__(self, execute_serial: bool = False):
        self.execute_serial = execute_serial

        # since failed tasks are retried, results[FAILED] can contain multiple entries for
        # a continuing failing task
        self.results: Dict[TaskResultState, List[TaskResult]] = defaultdict(list)

        self.failed_tasks: List[Task] = []

    @abstractmethod
    def calculate_tasks(self) -> List[Task]:
        """ """

    @abstractmethod
    def post_process(self):
        """ If something has to be done after the process is finished """

    def _process_task(self, task: Task) -> TaskResult:
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
            logger.info("Failed: %s", task)
            return TaskResult(task=task,
                              result=task.exception(exception=ex),
                              state=TaskResultState.FAILED)

    def _process(self):
        executor = ThreadExecutor[Task, TaskResult, TaskResult](
            processes=3,
            max_calls_per_sec=8,
            chunksize=3,
            execute_serial=self.execute_serial
        )
        executor.set_get_entries_function(self.calculate_tasks)
        executor.set_process_element_function(self._process_task)
        executor.set_post_process_chunk_function(lambda x: x)  # no process_chunk at this place

        results_all, self.failed_tasks = executor.execute()
        for entry in results_all:
            self.results[entry.state].append(entry)

        logger = logging.getLogger()
        for failed in self.failed_tasks:
            logger.warning("not able to process %s", failed)

        self.post_process()

    def process(self):
        self._process()
