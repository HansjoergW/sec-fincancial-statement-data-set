import os
from pathlib import Path

from secfsdstools.c_automation.task_framework import AbstractTask

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"



# --- methods of Abstract class ---------------------------------------------------------------
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

# todo: add tests to generally  check_by_timestamp or by folders.. maybe with a simple test task
