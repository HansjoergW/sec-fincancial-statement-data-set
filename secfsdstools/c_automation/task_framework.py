import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, List, Any

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
        self.results: List[TaskResult] = []

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

    def _process_parallel(self):
        executor = ThreadExecutor[Task, TaskResult, TaskResult](
            processes=3,
            max_calls_per_sec=8,
            chunksize=3,
            execute_serial=False
            # execute_serial=self.execute_serial
        )
        executor.set_get_entries_function(self.calculate_tasks)
        executor.set_process_element_function(self._process_task)
        executor.set_post_process_chunk_function(lambda x: x) # no process_chunk at this place

        problem: results enthalten auch fehlgeschlagene, die retried werden
        missing ist nur der Task, ohne Fehlermeldung

        entweder results liste bereiningen -> nur ein Success oder "Id" in missing
        results:List[TaskResult], missing: List[Task] = executor.execute()
        print("trial")

    def _process_serial(self):
        self.results = [self._process_task(task) for task in self.calculate_tasks()]

    def process(self):
        sollte serial nicht via ThreadExecutor gehen, damit retry gemanaged ist?
        if self.execute_serial:
            self._process_serial()
        else:
            self._process_parallel()

        self.post_process()
