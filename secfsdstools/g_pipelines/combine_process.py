"""
Module that combines the content from several input folders into a single DataBag.
It can handle either raw or joined databags.
"""
from pathlib import Path
from typing import List

from secfsdstools.c_automation.task_framework import AbstractProcess, Task, delete_temp_folders, \
    CheckByNewSubfoldersBaseTask, CheckByTimestampBaseTask, concat_bags, AbstractTask


class ConcatIfNewSubfolderTask(CheckByNewSubfoldersBaseTask):
    """

    """

    def __init__(self,
                 root_path: Path,
                 filter: str,
                 target_path: Path):
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

        The logic is executed, if new subfolders are available.
        Meaning, if a new folder in the root_path appears, it will add it to
        the already existing content in the target_path. So, if in the above examples, the
        subfolders 2010q1.zip ... 2024q3.zip were already concatenated in a previous run into the
        target_path and in a new run the subfolder 2024q4.zip is detected, it will load the existing
        content in the target_path, add the content of the 2024q4.zip and writes the result back
        into the target_path.
        This is being done by writing the name of the processed subfolders into a meta.inf file
        in the target_path.

        Args:
            root_path:
            filter:
            target_path:
        """
        super().__init__(
            root_path=root_path,
            filter=filter,
            target_path=target_path
        )

    def __str__(self) -> str:
        return f"ConcatIfNewSubfolderTask(root_path: {self.root_path}, filter: {self.filter})"

    def do_execution(self, paths_to_process: List[Path], target_path: Path):
        concat_bags(paths_to_concat=paths_to_process, target_path=target_path)


class ConcatIfChangedTimestampTask(CheckByTimestampBaseTask):
    """

    """

    def __init__(self,
                 root_path: Path,
                 filter: str,
                 target_path: Path):
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

        The logic will be executed, if a file/folder in the root_path has changed since the
        last processing.

        This is being done by writing the actual last modification timestamp of the root_path
        into the meta.inf file of the target_path.


        Args:
            root_path:
            filter:
            target_path:
        """
        super().__init__(
            root_path=root_path,
            filter=filter,
            target_path=target_path
        )

    def __str__(self) -> str:
        return f"ConcatIfChangedTimestampTask(root_path: {self.root_path}, filter: {self.filter})"

    def do_execution(self, paths_to_process: List[Path], target_path: Path):
        concat_bags(paths_to_concat=paths_to_process, target_path=target_path)


class ConcatProcess(AbstractProcess):

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
                 filter: str = "*",
                 check_by_timestamp: bool = False
                 ):
        super().__init__(execute_serial=False,
                         chunksize=0)

        self.root_path = Path(root_dir)
        self.target_path = Path(target_dir)
        self.filter = filter
        self.check_by_timestamp = check_by_timestamp

    def pre_process(self):
        delete_temp_folders(root_path=self.target_path.parent)

    def calculate_tasks(self) -> List[AbstractTask]:
        task: AbstractTask
        if self.check_by_timestamp:
            task = ConcatIfChangedTimestampTask(
                root_path=self.root_path,
                filter=self.filter,
                target_path=self.target_path,
            )
        else:
            task = ConcatIfNewSubfolderTask(
                root_path=self.root_path,
                filter=self.filter,
                target_path=self.target_path,
            )

        # since this is a one task process, we just check if there is really something to do
        if len(task.paths_to_process) > 0:
            return [task]
        return []
