import os
from pathlib import Path

from secfsdstools.c_automation.automation_utils import delete_temp_folders, get_latest_mtime

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH_1 = Path(CURRENT_DIR) / ".." / "_testdata" / "joined"
TESTDATA_PATH_2 = Path(CURRENT_DIR) / ".." / "_testdata" / "parquet_new"


def test_get_latest_mtime():
    # we just test if it is generally working
    latest_mtime: float = get_latest_mtime(root_paths=[TESTDATA_PATH_1, TESTDATA_PATH_2])
    assert latest_mtime > 0


def test_delete_temp_folders(tmp_path):
    tmp_dir1 = tmp_path / "tmp_dir1"
    tmp_dir2 = tmp_path / "tmp_dir2"
    dir1 = tmp_path / "dir1"

    tmp_dir1.mkdir()
    tmp_dir2.mkdir()
    dir1.mkdir()

    assert tmp_dir1.exists()
    assert tmp_dir2.exists()
    assert dir1.exists()

    delete_temp_folders(root_path=tmp_path)

    assert not tmp_dir1.exists()
    assert not tmp_dir2.exists()
    assert dir1.exists()
