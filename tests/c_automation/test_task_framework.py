from pathlib import Path

from secfsdstools.c_automation.task_framework import BaseTask
def test_get_iterator_position_name():
    assert BaseTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=0) == "e"
    assert BaseTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=1) == "d"
    assert BaseTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=2) == "c"
    assert BaseTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=3) == "b"
    assert BaseTask._get_iterator_position_name(path=Path("a/b/c/d/e"), star_position=4) == "a"


def test_start_position_from_end():
    assert BaseTask._star_position_from_end("/*") == 0
    assert BaseTask._star_position_from_end("/*/") == 0
    assert BaseTask._star_position_from_end("/*/BS") == 1
    assert BaseTask._star_position_from_end("*") == 0
    assert BaseTask._star_position_from_end("*/") == 0
    assert BaseTask._star_position_from_end("*/BS") == 1
    assert BaseTask._star_position_from_end("*/BS/") == 1
    assert BaseTask._star_position_from_end("*/BS/x") == 2
    assert BaseTask._star_position_from_end("*/BS/x/") == 2
    assert BaseTask._star_position_from_end("x/*") == 0
    assert BaseTask._star_position_from_end("x/*/") == 0
    assert BaseTask._star_position_from_end("x/*/BS") == 1
    assert BaseTask._star_position_from_end("x/*/BS/") == 1
    assert BaseTask._star_position_from_end("x/*/BS/x") == 2
    assert BaseTask._star_position_from_end("x/*/BS/x/") == 2


# todo: add tests to generally  check_by_timestamp or by folders.. maybe with a simple test task
