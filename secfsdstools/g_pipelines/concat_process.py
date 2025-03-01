"""
Module that combines the content from several input folders into a single DataBag.
"""
from pathlib import Path
from typing import List

from secfsdstools.c_automation.automation_utils import delete_temp_folders
from secfsdstools.c_automation.task_framework import CheckByNewSubfoldersMergeBaseTask, \
    CheckByTimestampMergeBaseTask, AbstractTask, \
    AbstractThreadProcess
from secfsdstools.g_pipelines.pipeline_utils import concat_bags_filebased, concat_bags


class ConcatIfNewSubfolderTask(CheckByNewSubfoldersMergeBaseTask):
    """
        Concats subfolders in root_path if a new subfolder was added.
    """

    def __init__(self,
                 root_path: Path,
                 pathfilter: str,
                 target_path: Path,
                 in_memory: bool = False):
        """
        Takes subfolders from the root_path (with applied pathfilter string) and
        concatenates them into a single DataBag (either Raw or Joined) into the target_path.

        The pathfilter string defines on which subfolder level actually does contain the data to be
        concatenated.

        E.g. if the pathfilter is just a "*" and the root_path looks like
        <pre>
        root_path
            2010q1.zip
            2010q2.zip
            ...
            2024q3.zip
        </pre>
        it would expect that the subfolders directly contain the databags.<br>
        If the pathfilter is defined as "*/BS" and the root_path looks like
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
        the already existing content in the target_path. So, if in the above example the
        subfolders 2010q1.zip ... 2024q3.zip already were concatenated in a previous run
        and in the next run the subfolder 2024q4.zip is detected, it will add the new subfolder
        to the existin gcontent in the target_path.

        This is being done by writing the name of the processed subfolders into a meta.inf file
        in the target_path.

        Args:
            root_path: root path to read that from
            pathfilter: pathfilter string that defines which subfolders in the root_path have
                        to be selected
            target_path: path to where the results have to be written
            in_memory: if true, the concatenation is being done in memory instead directly on the
                       filesystem. This can consume a lot of memory depending on the bag size.
        """
        super().__init__(
            root_path=root_path,
            pathfilter=pathfilter,
            target_path=target_path
        )
        self.in_memory = in_memory

    def __str__(self) -> str:
        return f"ConcatIfNewSubfolderTask(root_path: {self.root_path}, pathfilter: {self.filter})"

    def do_execution(self,
                     paths_to_process: List[Path],
                     target_path: Path,
                     tmp_path: Path):
        """
            defines the logic to be executed.
        Args:
            paths_to_process: lists of paths/folders that have to be processed
            target_path: the path where the result of the previous run was written
            tmp_path: target path to where a result has to be written
        """
        paths_to_concat = paths_to_process

        # if the target path exits, it must also be concatenated into the new result
        if target_path.exists():
            paths_to_concat = paths_to_process + [target_path]

        # concat and save to the tmp_path
        if self.in_memory:
            concat_bags(paths_to_concat=paths_to_concat,
                        target_path=tmp_path,
                        drop_duplicates_sub_df=True
                        )
        else:
            concat_bags_filebased(paths_to_concat=paths_to_concat,
                                  target_path=tmp_path,
                                  drop_duplicates_sub_df=True)


class ConcatIfChangedTimestampTask(CheckByTimestampMergeBaseTask):
    """
        Concats subfolders in root_path if something did change in any subfolder since the last
        run.
    """

    def __init__(self,
                 root_path: Path,
                 pathfilter: str,
                 target_path: Path,
                 in_memory: bool = False):
        """
        Takes subfolder from the root_path (with applied pathfilter string) and
        concatenates them into a single DataBag (either Raw or Joined) into the target_path.

        The pathfilter string defines on which subfolder level actually does contain the data to be
        concatenated.

        E.g. if the pathfilter is just a "*" and the root_path looks like
        <pre>
        root_path
            2010q1.zip
            2010q2.zip
            ...
            2024q3.zip
        </pre>
        it would expect that the subfolders directly contain the databags.<br>
        If the pathfilter is defined as "*/BS" and the root_path looks like
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
            root_path: root path to read that from
            pathfilter: pathfilter string that defines which subfolders in the root_path
                        have to be selected
            target_path: path to where the results have to be written
            in_memory: if true, the concatenation is being done in memory instead directly on the
                       filesystem. This can consume a lot of memory depending on the bag size.
        """
        super().__init__(
            root_path=root_path,
            pathfilter=pathfilter,
            target_path=target_path
        )
        self.in_memory = in_memory

    def __str__(self) -> str:
        return f"ConcatIfChangedTimestampTask(root_path: {self.root_path}," \
               f"pathfilter: {self.filter})"

    def do_execution(self,
                     paths_to_process: List[Path],
                     tmp_path: Path):
        """
        concats the bags in the paths_to_process and saves it in the tmp_path.

        Args:
            paths_to_process: lists of paths/folders that have to be processed
            tmp_path: path to where a result has to be written
        """
        if self.in_memory:
            concat_bags(paths_to_concat=paths_to_process,
                        target_path=tmp_path,
                        drop_duplicates_sub_df=True
                        )
        else:
            concat_bags_filebased(paths_to_concat=paths_to_process,
                                  target_path=tmp_path,
                                  drop_duplicates_sub_df=True)


class ConcatByChangedTimestampProcess(AbstractThreadProcess):
    """
    Process implementation that concatenates raw or joined databigs in a root_dir
    if something changed in any of the subfolders and saves the result in the target_dir.
    """

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
                 pathfilter: str = "*",
                 in_memory: bool = False
                 ):
        """
        Constructor.
        Args:
            root_dir: root_dir which contains the bags to be concatenated
            target_dir: target_dir to which the concatenated result has to be written
            pathfilter: pathfilter string to apply to select the bags/subfolders to be processed.
                    default is "*".
            in_memory: if true, the concatenation is being done in memory instead directly on the
                       filesystem. This can consume a lot of memory depending on the bag size.

        """
        super().__init__(execute_serial=False,
                         chunksize=0)

        self.root_path = Path(root_dir)
        self.target_path = Path(target_dir)
        self.filter = pathfilter
        self.in_memory = in_memory

    def pre_process(self):
        """
        Executed before the actual process and cleans up any "tmp" folders within the target,
        in case of a failing previous processing.
        """
        delete_temp_folders(root_path=self.target_path.parent)

    def calculate_tasks(self) -> List[AbstractTask]:
        """
            Calculates the tasks that actually execute the logic.
            This is a single task process, so just a single task is being created.

        Returns:
            List[AbstractTask]: Task to be executed
        """
        task = ConcatIfChangedTimestampTask(
            root_path=self.root_path,
            pathfilter=self.filter,
            target_path=self.target_path,
            in_memory=self.in_memory
        )

        # since this is a one task process, we just check if there is really something to do
        if len(task.paths_to_process) > 0:
            return [task]
        return []


class ConcatByNewSubfoldersProcess(AbstractThreadProcess):
    """
    Process implementation that concatenates raw or joined databags in a root_dir
    if a new bag/subfolder was added to the root_dir and saves the result in the target_dir.
    """

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
                 pathfilter: str = "*",
                 in_memory: bool = False
                 ):
        """
        Constructor.
        Args:
            root_dir: root_dir which contains the bags to be concatenated
            target_dir: target_dir to which the concatenated result has to be written
            pathfilter: pathfilter string to apply to select the bags/subfolders to be processed.
                    default is "*".
            in_memory: if true, the concatenation is being done in memory instead directly on the
                       filesystem. This can consume a lot of memory depending on the bag size.

        """
        super().__init__(execute_serial=False,
                         chunksize=0)

        self.root_path = Path(root_dir)
        self.target_path = Path(target_dir)
        self.filter = pathfilter
        self.in_memory = in_memory

    def pre_process(self):
        """
        Executed before the actual process and cleans up any "tmp" folders within the target,
        in case of a failing previous processing.
        """
        delete_temp_folders(root_path=self.target_path.parent)

    def calculate_tasks(self) -> List[AbstractTask]:
        """
            Calculates the tasks that actually execute the logic.
            This is a single task process, so just a single task is being created.

        Returns:
            List[AbstractTask]: Task to be executed
        """
        task = ConcatIfNewSubfolderTask(
            root_path=self.root_path,
            pathfilter=self.filter,
            target_path=self.target_path,
            in_memory=self.in_memory
        )

        # since this is a one task process, we just check if there is really something to do
        if len(task.paths_to_process) > 0:
            return [task]
        return []
