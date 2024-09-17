from abc import ABC, abstractmethod
from typing import Protocol, List, Tuple
from secfsdstools.a_utils.parallelexecution import ThreadExecutor

class Task(Protocol):

    class Task:
        def prepare(self):
            """ """

        def execute(self):
            """ """

        def commit(self):
            """ """

        def post_commit(self):
            """ """

        def exception(self, exception):
            """ """


class AbstractProcess(ABC):

    def __init__(self,
                 execute_serial: bool = False):
        self.execute_serial = execute_serial

    @abstractmethod
    def calculate_tasks(self) -> List[Task]:
        """ """

    def process_task(self, task: Task):
        pass

    def process(self):
        tasks: List[Task] = self.calculate_tasks()

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

Fragen:
- Errorhandling
- Typisierung von ThreadExecutor hat vermutlich eine Bedeutung, wie passt das mit einem generischen
  AbstractProcess zusammen
- Nicht alles wird parallel verarbeitet (z.B. indexing ist nicht parallel.)