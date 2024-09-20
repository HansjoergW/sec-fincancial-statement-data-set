from abc import ABC, abstractmethod
from typing import Protocol, List

from secfsdstools.a_utils.parallelexecution import ThreadExecutor


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


besser ein result object

Result:
  task: Task
  message: str
  state: Any

und dann zweilisten in Process
mit success und mit failure, so dass man im postproces noch etwas machen könnte


class AbstractProcess(ABC):

    def __init__(self, execute_serial: bool = False):
        self.execute_serial = execute_serial

    @abstractmethod
    def calculate_tasks(self) -> List[Task]:
        """ """

    @abstractmethod
    def post_process(self):
        """ If something has to be done after the process is finished """

    def process_task(self, task: Task) -> str:
        try:
            task.prepare()
            task.execute()
            return task.commit()
        except Exception as ex:  # pylint: disable=W0703
            # we want to catch everything here.
            return task.exception(exception=ex)

    def _process_parallel(self):
        executor = ThreadExecutor[Task, str, type(None)](
            processes=3,
            max_calls_per_sec=8,
            chunksize=3,
            execute_serial=False
            # execute_serial=self.execute_serial
        )
        executor.set_get_entries_function(self.calculate_tasks)
        executor.set_process_element_function(self.process_task)
        executor.set_post_process_chunk_function(lambda x: x)

        self.result = executor.execute()

    def _process_serial(self):
        for task in self.calculate_tasks():
            hier müsste man die results collecten...
            self.process_task(task)

    def process(self):

        if self.execute_serial:
            self._process_serial()
        else:
            self._process_parallel()

        self.post_process()
# Fragen:
# - Errorhandling
# - Typisierung von ThreadExecutor hat vermutlich eine Bedeutung, wie passt das mit einem generischen
#   AbstractProcess zusammen
