import os
import shutil
from pathlib import Path

from secfsdstools.c_automation.task_framework import AbstractProcess
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.g_pipelines.combine_process import CombineTask

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / "_testdata"


def test_get_iterator_position_name():
    assert CombineTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=0) == "e"
    assert CombineTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=1) == "d"
    assert CombineTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=2) == "c"
    assert CombineTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=3) == "b"
    assert CombineTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=4) == "a"


def test_start_position_from_end():
    assert CombineTask._star_position_from_end("/*") == 0
    assert CombineTask._star_position_from_end("/*/") == 0
    assert CombineTask._star_position_from_end("/*/BS") == 1
    assert CombineTask._star_position_from_end("*") == 0
    assert CombineTask._star_position_from_end("*/") == 0
    assert CombineTask._star_position_from_end("*/BS") == 1
    assert CombineTask._star_position_from_end("*/BS/") == 1
    assert CombineTask._star_position_from_end("*/BS/x") == 2
    assert CombineTask._star_position_from_end("*/BS/x/") == 2
    assert CombineTask._star_position_from_end("x/*") == 0
    assert CombineTask._star_position_from_end("x/*/") == 0
    assert CombineTask._star_position_from_end("x/*/BS") == 1
    assert CombineTask._star_position_from_end("x/*/BS/") == 1
    assert CombineTask._star_position_from_end("x/*/BS/x") == 2
    assert CombineTask._star_position_from_end("x/*/BS/x/") == 2


def test_direct_sub_directory_collect(tmp_path):
    """
        tmp_path/all -> empty

        tmp_path/quarter/2010q1.zip
        tmp_path/quarter/2010q2.zip
        tmp_path/quarter/2010q3.zip
    """

    # First Part: Test against empty target folder
    # prepare for first step -> copy initial 3 folders to temp folder
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder)

    # execute task
    task = CombineTask(
        root_path=tmp_path / "quarter",
        bag_type="joined",
        filter="*",
        target_path=tmp_path / "all"
    )

    result = AbstractProcess.process_task(task)

    # check results
    assert len(task.missing_paths) == 3
    assert result.result == "success"
    assert task.target_path.exists()
    assert task.target_path.is_dir()
    assert task.meta_inf_file.exists()

    meta_inf_content = task.read_metainf_content()
    assert len(set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip"}) == 0

    bag = JoinedDataBag.load(str(task.target_path))
    assert bag.sub_df.shape == (2171, 36)

    # Second Part: Add another folder
    # prepare for second step -> copy new folder to temp folder
    folder = "2010q4.zip"
    shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder)

    # execute task
    task2 = CombineTask(
        root_path=tmp_path / "quarter",
        bag_type="joined",
        filter="*",
        target_path=tmp_path / "all"
    )

    result2 = AbstractProcess.process_task(task2)
    # check results
    assert len(task2.missing_paths) == 1
    assert result2.result == "success"
    assert task2.target_path.exists()
    assert task2.target_path.is_dir()
    assert task2.meta_inf_file.exists()

    meta_inf_content = task2.read_metainf_content()
    assert len(
        set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip", "2010q4.zip"}) == 0

    bag = JoinedDataBag.load(str(task2.target_path))
    assert bag.sub_df.shape == (3585, 36)


def test_child_sub_directory_collect(tmp_path):
    """
    tmp_path/all_by_stmt/BS -> empty

    tmp_path/quarter/2010q1.zip/BS
    tmp_path/quarter/2010q2.zip/BS
    tmp_path/quarter/2010q3.zip/BS

    -> metainf has to contain 2010q1.zip, ...
    """

    # First Part: Test against empty target folder
    # prepare for first step -> copy initial 3 folders to temp folder
    # with the sub directory BS
    # the data we copy is not actually BS only data, but that doesn't matter for the test
    folders = ["2010q1.zip", "2010q2.zip", "2010q3.zip"]
    for folder in folders:
        shutil.copytree(src=TESTDATA_PATH / "joined" / folder,
                        dst=tmp_path / "quarter" / folder / "BS")

    # execute task
    task = CombineTask(
        root_path=tmp_path / "quarter",
        bag_type="joined",
        filter="*/BS",
        target_path=tmp_path / "all"
    )

    result = AbstractProcess.process_task(task)

    # check results
    assert len(task.missing_paths) == 3
    assert result.result == "success"
    assert task.target_path.exists()
    assert task.target_path.is_dir()
    assert task.meta_inf_file.exists()

    meta_inf_content = task.read_metainf_content()
    assert len(set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip"}) == 0

    bag = JoinedDataBag.load(str(task.target_path))
    assert bag.sub_df.shape == (2171, 36)

    # Second Part: Add another folder
    # prepare for second step -> copy new folder to temp folder
    folder = "2010q4.zip"
    shutil.copytree(src=TESTDATA_PATH / "joined" / folder, dst=tmp_path / "quarter" / folder / "BS")

    # execute task
    task2 = CombineTask(
        root_path=tmp_path / "quarter",
        bag_type="joined",
        filter="*/BS",
        target_path=tmp_path / "all"
    )

    result2 = AbstractProcess.process_task(task2)
    # check results
    assert len(task2.missing_paths) == 1
    assert result2.result == "success"
    assert task2.target_path.exists()
    assert task2.target_path.is_dir()
    assert task2.meta_inf_file.exists()

    meta_inf_content = task2.read_metainf_content()
    assert len(
        set(meta_inf_content) - {"2010q1.zip", "2010q2.zip", "2010q3.zip", "2010q4.zip"}) == 0

    bag = JoinedDataBag.load(str(task2.target_path))
    assert bag.sub_df.shape == (3585, 36)
