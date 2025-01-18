import os
import shutil
from pathlib import Path

from secfsdstools.c_automation.task_framework import AbstractProcess, TaskResultState
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.g_pipelines.concat_process import ConcatIfNewSubfolderTask, ConcatIfChangedTimestampTask, \
    ConcatByNewSubfoldersProcess, ConcatByChangedTimestampProcess


CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"


def test_direct_sub_directory_collect(tmp_path):
    """
        tmp_path/all -> empty

        tmp_path/quarter/2010q1.zip
        tmp_path/quarter/2010q2.zip
        tmp_path/quarter/2010q3.zip
    """

    target_dir = str(tmp_path / "all")
    root_dir = str(tmp_path / "quarter")

    # First Part: Test against empty target folder
    # prepare for first step -> copy initial 3 folders to temp folder
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder)

    # execute process
    process = ConcatByNewSubfoldersProcess(
        root_dir=root_dir,
        pathfilter="*",
        target_dir=target_dir
    )

    process.process()

    task_result = process.results[TaskResultState.SUCCESS][0]
    task: ConcatIfNewSubfolderTask = task_result.task

    # check results
    assert len(task.paths_to_process) == 3
    assert task_result.result == "success"
    assert task.target_path.exists()
    assert task.target_path.is_dir()
    assert task.meta_inf_file.exists()

    meta_inf_content = task.read_metainf_content()
    assert len(set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip"}) == 0

    bag = JoinedDataBag.load(str(task.target_path))
    assert bag.sub_df.shape == (2429, 36)

    # Second Part: Add another folder
    # prepare for second step -> copy new folder to temp folder
    folder = "2010q4.zip"
    shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder)

    # execute process
    process2 = ConcatByNewSubfoldersProcess(
        root_dir=root_dir,
        pathfilter="*",
        target_dir=target_dir
    )

    process2.process()

    task_result2 = process2.results[TaskResultState.SUCCESS][0]
    task2: ConcatIfNewSubfolderTask = task_result2.task

    # check results
    assert len(task2.paths_to_process) == 1
    assert task_result2.result == "success"
    assert task2.target_path.exists()
    assert task2.target_path.is_dir()
    assert task2.meta_inf_file.exists()

    meta_inf_content = task2.read_metainf_content()
    assert len(
        set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip", "2010q4.zip"}) == 0

    bag = JoinedDataBag.load(str(task2.target_path))
    assert bag.sub_df.shape == (3903, 36)


def test_child_sub_directory_collect(tmp_path):
    """
    tmp_path/all_by_stmt/BS -> empty

    tmp_path/quarter/2010q1.zip/BS
    tmp_path/quarter/2010q2.zip/BS
    tmp_path/quarter/2010q3.zip/BS

    -> metainf has to contain 2010q1.zip, ...
    """

    target_dir = str(tmp_path / "all")
    root_dir = str(tmp_path / "quarter")

    # First Part: Test against empty target folder
    # prepare for first step -> copy initial 3 folders to temp folder
    # with the sub directory BS
    # the data we copy is not actually BS only data, but that doesn't matter for the test
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder,
                        dst=tmp_path / "quarter" / folder / "BS")

    # execute process
    process = ConcatByNewSubfoldersProcess(
        root_dir=root_dir,
        pathfilter="*/BS",
        target_dir=target_dir
    )

    process.process()

    task_result = process.results[TaskResultState.SUCCESS][0]
    task: ConcatIfNewSubfolderTask = task_result.task

    # check results
    assert len(task.paths_to_process) == 3
    assert task_result.result == "success"
    assert task.target_path.exists()
    assert task.target_path.is_dir()
    assert task.meta_inf_file.exists()

    meta_inf_content = task.read_metainf_content()
    assert len(set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip"}) == 0

    bag = JoinedDataBag.load(str(task.target_path))
    assert bag.sub_df.shape == (2429, 36)

    # Second Part: Add another folder
    # prepare for second step -> copy new folder to temp folder
    folder = "2010q4.zip"
    shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder / "BS")

    # execute process
    process2 = ConcatByNewSubfoldersProcess(
        root_dir=root_dir,
        pathfilter="*/BS",
        target_dir=target_dir
    )

    process2.process()

    task_result2 = process2.results[TaskResultState.SUCCESS][0]
    task2: ConcatIfNewSubfolderTask = task_result2.task

    # check results
    assert len(task2.paths_to_process) == 1
    assert task_result2.result == "success"
    assert task2.target_path.exists()
    assert task2.target_path.is_dir()
    assert task2.meta_inf_file.exists()

    meta_inf_content = task2.read_metainf_content()
    assert len(
        set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip", "2010q4.zip"}) == 0

    bag = JoinedDataBag.load(str(task2.target_path))
    assert bag.sub_df.shape == (3903, 36)


def test_changed_content_collect(tmp_path):
    """
        tmp_path/all -> empty

        tmp_path/quarter/2010q1.zip
        tmp_path/quarter/2010q2.zip
        tmp_path/quarter/2010q3.zip
    """

    target_dir = str(tmp_path / "all")
    root_dir = str(tmp_path / "quarter")

    # First Part: Test against empty target folder
    # prepare for first step -> copy initial 3 folders to temp folder
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder)

    # execute process
    process = ConcatByChangedTimestampProcess(
        root_dir=root_dir,
        pathfilter="*",
        target_dir=target_dir
    )

    process.process()

    task_result = process.results[TaskResultState.SUCCESS][0]
    task: ConcatIfNewSubfolderTask = task_result.task

    # check results
    assert len(task.paths_to_process) == 3
    assert task_result.result == "success"
    assert task.target_path.exists()
    assert task.target_path.is_dir()
    assert task.meta_inf_file.exists()

    meta_inf_content = task.read_metainf_content()
    assert len(set(meta_inf_content)) == 1 # we expect a single timestamp
    ts_initial: float = float(meta_inf_content[0])

    bag = JoinedDataBag.load(str(task.target_path))
    assert bag.sub_df.shape == (2429, 36)

    # second part: change the content
    # we overwrite the content of the 2010q3 with q4, to simulate an update in the folder
    shutil.copytree(src=TESTDATA_PATH / "joined" / "2010q4.zip",
                    dst=tmp_path / "quarter" / "2010q3.zip", dirs_exist_ok=True)

    # execute task
    # execute process
    process2 = ConcatByChangedTimestampProcess(
        root_dir=root_dir,
        pathfilter="*",
        target_dir=target_dir
    )

    process2.process()

    task_result2 = process2.results[TaskResultState.SUCCESS][0]
    task2: ConcatIfNewSubfolderTask = task_result2.task
    # check results
    assert len(task2.paths_to_process) == 3
    assert task_result2.result == "success"
    assert task2.target_path.exists()
    assert task2.target_path.is_dir()
    assert task2.meta_inf_file.exists()

    meta_inf_content = task2.read_metainf_content()
    assert len(meta_inf_content) == 1  # we expect a single timestamp
    ts_changed: float = float(meta_inf_content[0])

    # the new timestamp should be later than the initial ts
    assert ts_changed > ts_initial

    bag = JoinedDataBag.load(str(task2.target_path))
    assert bag.sub_df.shape == (2491, 36)
