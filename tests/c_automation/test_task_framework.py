import os
import shutil
from pathlib import Path
from typing import List

from secfsdstools.c_automation.task_framework import (
    AbstractParallelProcess,
    AbstractProcess,
    AbstractProcessPoolProcess,
    AbstractTask,
    AbstractThreadProcess,
    CheckByNewSubfoldersMergeBaseTask,
    CheckByTimestampMergeBaseTask,
    Task,
    TaskResultState,
)

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"


# --- static methods of class AbstractTask -------------------------------------------------------
def test_get_iterator_position_name():
    assert AbstractTask._get_star_position_name(path=Path("a/b/c/d/e"), star_position=0) == "e"
    assert AbstractTask._get_star_position_name(path=Path("a/b/c/d/e"), star_position=1) == "d"
    assert AbstractTask._get_star_position_name(path=Path("a/b/c/d/e"), star_position=2) == "c"
    assert AbstractTask._get_star_position_name(path=Path("a/b/c/d/e"), star_position=3) == "b"
    assert AbstractTask._get_star_position_name(path=Path("a/b/c/d/e"), star_position=4) == "a"


def test_start_position_from_end():
    assert AbstractTask._star_position_from_end("/*") == 0
    assert AbstractTask._star_position_from_end("/*/") == 0
    assert AbstractTask._star_position_from_end("/*/BS") == 1
    assert AbstractTask._star_position_from_end("*") == 0
    assert AbstractTask._star_position_from_end("*/") == 0
    assert AbstractTask._star_position_from_end("*/BS") == 1
    assert AbstractTask._star_position_from_end("*/BS/") == 1
    assert AbstractTask._star_position_from_end("*/BS/x") == 2
    assert AbstractTask._star_position_from_end("*/BS/x/") == 2
    assert AbstractTask._star_position_from_end("x/*") == 0
    assert AbstractTask._star_position_from_end("x/*/") == 0
    assert AbstractTask._star_position_from_end("x/*/BS") == 1
    assert AbstractTask._star_position_from_end("x/*/BS/") == 1
    assert AbstractTask._star_position_from_end("x/*/BS/x") == 2
    assert AbstractTask._star_position_from_end("x/*/BS/x/") == 2


# --- test CheckByTimestampMergeBaseTask -------------------------------------------------------
class MyByTSTask(CheckByTimestampMergeBaseTask):
    called_paths_to_process: List[Path]
    called_tmp_path: Path

    def do_execution(self,
                     paths_to_process: List[Path],
                     tmp_path: Path):
        self.called_paths_to_process = paths_to_process
        self.called_tmp_path = tmp_path


def test_checkbytimestamptask(tmp_path):
    # First Part: Test against empty target folder
    # prepare for first step -> copy initial 3 folders to temp folder
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder)

    # execute task
    task = MyByTSTask(
        root_path=tmp_path / "quarter",
        pathfilter="*",
        target_path=tmp_path / "all",
    )

    assert task.has_work_todo()

    result = AbstractParallelProcess.process_task(task)

    assert len(task.called_paths_to_process) == 3
    assert result.result == "success"
    assert task.target_path.exists()
    assert task.target_path.is_dir()
    assert task.meta_inf_file.exists()
    assert not task.called_tmp_path.exists()  # shouldn't be there anymore

    meta_inf_content = task.read_metainf_content()
    assert len(meta_inf_content) == 1  # we expect a single timestamp
    ts_initial: float = float(meta_inf_content[0])

    # Test 2: we check what happens if there was now change
    task_nochange = MyByTSTask(
        root_path=tmp_path / "quarter",
        pathfilter="*",
        target_path=tmp_path / "all",
    )

    # since there are no changes, no work should be done
    assert not task_nochange.has_work_todo()

    # Test 3: change the content
    # we overwrite the content of the 2010q3 with q4, to force an update in the folder
    shutil.copytree(src=TESTDATA_PATH / "joined" / "2010q4.zip",
                    dst=tmp_path / "quarter" / "2010q3.zip", dirs_exist_ok=True)

    task_changed = MyByTSTask(
        root_path=tmp_path / "quarter",
        pathfilter="*",
        target_path=tmp_path / "all",
    )

    assert task_changed.has_work_todo()

    result_task_changed = AbstractParallelProcess.process_task(task_changed)

    # we expect the same thant it was with the initial task
    assert len(task_changed.called_paths_to_process) == 3
    assert result_task_changed.result == "success"
    assert task_changed.target_path.exists()
    assert task_changed.target_path.is_dir()
    assert task_changed.meta_inf_file.exists()
    assert not task_changed.called_tmp_path.exists()  # shouldn't be there anymore

    meta_inf_content = task_changed.read_metainf_content()
    assert len(meta_inf_content) == 1  # we expect a single timestamp
    ts_changed: float = float(meta_inf_content[0])

    # the new timestamp should be later than the initial ts
    assert ts_changed > ts_initial


# --- test CheckByNewSubfoldersMergeBaseTask -------------------------------------------------------
class MyByNewSubfoldersTask(CheckByNewSubfoldersMergeBaseTask):
    called_paths_to_process: List[Path]
    called_target_path: Path
    called_tmp_path: Path

    def do_execution(self,
                     paths_to_process: List[Path],
                     target_path: Path,
                     tmp_path: Path):
        self.called_paths_to_process = paths_to_process
        self.called_target_path = target_path
        self.called_tmp_path = tmp_path


def test_checkbynewsubfoldertask(tmp_path):
    # Test1: Test against empty target folder

    # prepare for first step -> copy initial 3 folders to temp folder
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder)

    # execute task
    task = MyByNewSubfoldersTask(
        root_path=tmp_path / "quarter",
        pathfilter="*",
        target_path=tmp_path / "all",
    )

    assert task.has_work_todo()

    result = AbstractParallelProcess.process_task(task)

    assert len(task.called_paths_to_process) == 3
    assert result.result == "success"
    assert task.target_path.exists()
    assert task.target_path.is_dir()
    assert task.meta_inf_file.exists()
    assert not task.called_tmp_path.exists()  # shouldn't be there anymore

    meta_inf_content = task.read_metainf_content()
    assert len(meta_inf_content) == 3  # we expect a single timestamp
    assert len(set(meta_inf_content) - set(folders)) == 0

    # Test 2: we check what happens if there was now change
    task_nochange = MyByNewSubfoldersTask(
        root_path=tmp_path / "quarter",
        pathfilter="*",
        target_path=tmp_path / "all",
    )

    # since there are no changes, no work should be done
    assert not task_nochange.has_work_todo()

    # Test 3: add a new folder
    # we overwrite the content of the 2010q3 with q4, to force an update in the folder
    shutil.copytree(src=TESTDATA_PATH / "joined" / "2010q4.zip",
                    dst=tmp_path / "quarter" / "2010q4.zip")

    task_changed = MyByNewSubfoldersTask(
        root_path=tmp_path / "quarter",
        pathfilter="*",
        target_path=tmp_path / "all",
    )

    assert task_changed.has_work_todo()

    result_task_changed = AbstractParallelProcess.process_task(task_changed)

    # we expect the same thant it was with the initial task
    assert len(task_changed.called_paths_to_process) == 1
    assert task_changed.called_target_path.name == "all"
    assert result_task_changed.result == "success"
    assert task_changed.target_path.exists()
    assert task_changed.target_path.is_dir()
    assert task_changed.meta_inf_file.exists()
    assert not task_changed.called_tmp_path.exists()  # shouldn't be there anymore

    meta_inf_content = task_changed.read_metainf_content()
    assert len(meta_inf_content) == 4  # we expect a single timestamp
    assert len(
        set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip", "2010q4.zip"}) == 0


# --- test AbstractThreadProcess -------------------------------------------------------
class MyThreadProcess(AbstractThreadProcess):

    def __init__(self, base_path: Path):
        super().__init__()
        self.base_path = base_path

    def calculate_tasks(self) -> List[Task]:
        task = MyByNewSubfoldersTask(
            target_path=self.base_path / "target",
            root_path=self.base_path / "root",
            pathfilter="*"
        )
        if task.has_work_todo():
            return [task]

        return []


def test_abstractthreadprocess(tmp_path):
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "root" / folder)

    process = MyThreadProcess(tmp_path)
    process.process()

    assert len(process.results) == 1
    assert len(process.results[TaskResultState.SUCCESS]) == 1


# --- test AbstractProcessPoolProcess -------------------------------------------------------

class MyProcessPoolProcess(AbstractProcessPoolProcess):

    def __init__(self, base_path: Path):
        super().__init__()
        self.base_path = base_path

    def calculate_tasks(self) -> List[Task]:
        task = MyByNewSubfoldersTask(
            target_path=self.base_path / "target",
            root_path=self.base_path / "root",
            pathfilter="*"
        )
        if task.has_work_todo():
            return [task]
        return []


def test_abstractprocesspoolprocess(tmp_path):
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "root" / folder)

    process = MyProcessPoolProcess(tmp_path)
    process.process()

    assert len(process.results) == 1
    assert len(process.results[TaskResultState.SUCCESS]) == 1
