from typing import List

from secfsdstools.a_utils.parallelexecution import ParallelExecutor


def test_parallelexcution():
    # simple example to use it
    class MyTestClass:
        def __init__(self):
            self.data_list = [str(x) for x in range(500)]
            self.was_read = [False]
            self.result = None

        def get_unprocessed_entries(self) -> List[str]:
            if self.was_read[0] == False:
                self.was_read[0] = True
                return self.data_list
            return []

        def process_element(self, input: str) -> str:
            return "0" + str(input)

        def post_process(self, input: List[str]) -> List[str]:
            return input

        def process(self):
            executor = ParallelExecutor[str, str, str](max_calls_per_sec=250)
            executor.set_get_entries_function(self.get_unprocessed_entries)
            executor.set_process_element_function(self.process_element)
            executor.set_post_process_chunk_function(self.post_process)

            self.result = executor.execute()

    processor = MyTestClass()
    processor.process()
    processed, missing = processor.result

    assert len(processed) == 500
    assert len(missing) == 0
