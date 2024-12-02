"""
Module that combines the content from several input folders into a single DataBag.
It can handle either raw or joined databags.
"""
from pathlib import Path
from typing import List

from secfsdstools.c_automation.task_framework import AbstractProcess, Task, delete_temp_folders, \
    BaseTask
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag


class CombineTask(BaseTask):
    """

    """
    def __init__(self,
                 root_path: Path,
                 filter: str,
                 bag_type: str,  # raw or joined
                 target_path: Path,
                 check_by_timestamp: bool):
        """
        Takes subfolder from the root_path (with applied filter string) and concatenates them into
        a single DataBag (either Raw or Joined, defined by the bag_type) into the target_path.

        The filter string defines on which subfolder level actually does contain the data to be
        concatenated.

        E.g. if the filter is just a "*" and the root_path looks like
        <pre>
        root_path
            2010q1.zip
            2010q2.zip
            ...
            2024q3.zip
        </pre>
        it would expect that the subfolders directly contain the databags.<br>
        If the filter is defined as "*/BS" and the root_path looks like
        <pre>
        root_path
            2010q1.zip
              BS
              CF
              IS
            2010q2.zip
              BS
              CF
              IS
            ...
            2024q3.zip
              BS
              CF
              IS
        </pre>

        It would concatenate all BS subfolders into the target path.<br>

        check_by_timestamp defines how changes between runs are detected. If it is False, it
        looks for new subfolders. Meaning, if a new in the root_path appears, it will add it to
        the already existing content in the target_path. So, if in the above examples, the
        subfolders 2010q1.zip ... 2024q3.zip were already concatenated in a previous run into the
        target_path and in a new run the subfolder 2024q4.zip is detected, it will load the existing
        content in the target_path, add the content of the 2024q4.zip and writes the result back
        into the target_path.
        This is being done by writing the name of the processed subfolders into a meta.inf file
        in the target_path.

        If check_by_timestamp is True, it will re-concatenate, if any file or subfile has changed
        in the root_path since the last run.
        This is being done by writing the actual last modification timestamp of the root_path
        into the meta.inf file of the target_path.


        Args:
            root_path:
            filter:
            bag_type:
            target_path:
            check_by_timestamp:
        """
        super().__init__(
            root_path=root_path,
            filter=filter,
            target_path=target_path,
            check_by_timestamp=check_by_timestamp
        )
        self.bag_type = bag_type

    def __str__(self) -> str:
        return f"CombineTask(root_path: {self.root_path}, filter: {self.filter})"

    # def do_execution(self, paths_to_process: List[Path]):
    #     if self.bag_type.lower() == "raw":
    #         self._execute_raw(paths_to_process)
    #     elif self.bag_type.lower() == "joined":
    #         self._execute_joined(paths_to_process)
    #     else:
    #         raise ValueError("bag_type must be either raw or joined")
    #
    # def _execute_raw(self, paths: List[Path]):
    #     all_bags = [RawDataBag.load(str(path)) for path in paths]
    #
    #     all_bag: RawDataBag = RawDataBag.concat(all_bags, drop_duplicates_sub_df=True)
    #     all_bag.save(target_path=str(self.tmp_path))
    #
    # def _execute_joined(self, paths: List[Path]):
    #     all_bags = [JoinedDataBag.load(str(path)) for path in paths]
    #
    #     all_bag: JoinedDataBag = JoinedDataBag.concat(all_bags, drop_duplicates_sub_df=True)
    #     all_bag.save(target_path=str(self.tmp_path))


class CombineProcess(AbstractProcess):

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
                 bag_type: str,  # raw or joined
                 filter: str = "*",
                 check_by_timestamp: bool = False
                 ):
        super().__init__(execute_serial=False,
                         chunksize=0)

        self.root_path = Path(root_dir)
        self.target_path = Path(target_dir)
        self.bag_type = bag_type
        self.filter = filter
        self.check_by_timestamp = check_by_timestamp

    def pre_process(self):
        delete_temp_folders(root_path=self.target_path.parent)

    def calculate_tasks(self) -> List[Task]:
        task = CombineTask(
            root_path=self.root_path,
            bag_type=self.bag_type,
            filter=self.filter,
            target_path=self.target_path,
            check_by_timestamp=self.check_by_timestamp
        )
        # since this is a one task process, we just check if there is really something to do
        if len(task.paths_to_process) > 0:
            return [task]
        return []
