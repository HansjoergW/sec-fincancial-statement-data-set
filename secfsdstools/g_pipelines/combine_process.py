from pathlib import Path
from typing import List

from secfsdstools.c_automation.task_framework import AbstractProcess, Task, delete_temp_folders, \
    BaseTask
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag


class CombineTask(BaseTask):

    def __init__(self,
                 root_path: Path,
                 filter: str,
                 bag_type: str,  # raw or joined
                 target_path: Path,
                 check_by_timestamp: bool):
        super().__init__(
            root_path=root_path,
            filter=filter,
            target_path=target_path,
            check_by_timestamp=check_by_timestamp
        )
        self.bag_type = bag_type

    def __str__(self) -> str:
        return f"CombineTask(root_path: {self.root_path}, filter: {self.filter})"

    def do_execution(self, paths_to_process: List[Path]):
        if self.bag_type.lower() == "raw":
            self._execute_raw(paths_to_process)
        elif self.bag_type.lower() == "joined":
            self._execute_joined(paths_to_process)
        else:
            raise ValueError("bag_type must be either raw or joined")

    def _execute_raw(self, paths: List[Path]):
        all_bags = [RawDataBag.load(str(path)) for path in paths]

        all_bag: RawDataBag = RawDataBag.concat(all_bags, drop_duplicates_sub_df=True)
        all_bag.save(target_path=str(self.tmp_path))

    def _execute_joined(self, paths: List[Path]):
        all_bags = [JoinedDataBag.load(str(path)) for path in paths]

        all_bag: JoinedDataBag = JoinedDataBag.concat(all_bags, drop_duplicates_sub_df=True)
        all_bag.save(target_path=str(self.tmp_path))


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
