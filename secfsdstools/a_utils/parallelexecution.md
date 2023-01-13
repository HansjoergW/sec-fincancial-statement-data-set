Module secfsdstools.a_utils.parallelexecution
=============================================
Helper Utils to execute tasks in parallel.

using pathos.multiprocessing instead of multiprocessing, so that method functions can be used
-> pypi.org "pathos"

Classes
-------

`ParallelExecutor(processes: int = 2, chunksize: int = 100, max_calls_per_sec: int = 0, intend: str = '    ', execute_serial: bool = False)`
:   this helper class supports in parallel processing of entries that are provided
    as a list, for instance to     download and process data.. like downloading and
    processing sec filing reports.
    It mainly takes care about the following things:
    - it throttles the amount of calls
      for instance, you can define how many call should be made at max within a second.
      When you download content from the SEC, you may only send 10 request per second.
    - retry handling
      especially when reading data from the web, it is normal that some requests might
      end up with an exception. If that happens, the logic retries failed entries.
    - processing in chunks
      you can define the chunksize after which the state is updated. For instance
      when having to download and process a few thousand reports, you can make sure
      that the current state is stored every 100 reports. Therefore in case
      something "bigger" happens, at least the work that already was done is kept.
    
    
    How to use it
    You need to define 3 functions.
    The get_entries_function returnes a list with the entries that need to be processed.
    The process_element_function is the logic that processes a single element from the
    entries and returns the processed content.
    The post_process_chunk_function receives a list of processed entries. you use this
    function to update the processed entries, so that in the next call to
    get_entries_function, these entries will not be part of
    
    :param processes: number of parallel processes, default is cpu_count
    :param chunksize: size of chunk - think of it as a commit, default is 100
    :param max_calls_per_sec: how many calls may be made per second (for all processes),
        default is 0, meaning no limit
    :param execute_serial: for easier debugging, this flag ensures that all data are
           processed in the main thread

    ### Ancestors (in MRO)

    * typing.Generic

    ### Methods

    `execute(self) ‑> Tuple[List[~OT], List[~IT]]`
    :   starts the parallel processing and returns the results.
        
        :return: tuple with two lists: the first are the processed entries,
                 the sedond list are the entries that couldn't be processed

    `set_get_entries_function(self, get_entries: Callable[[], List[~IT]])`
    :   set the function which returns the list of the items that have not been processed.
        :param get_entries: function that returns the items to be processed

    `set_post_process_chunk_function(self, post_process: Callable[[List[~PT]], List[~OT]])`
    :   set the function that receives a list of processed elements unad updates
        the state of these elements accordingly
        :param post_process:

    `set_process_element_function(self, process_element: Callable[[~IT], ~PT])`
    :   set the function that processes a single element and returns the processed element