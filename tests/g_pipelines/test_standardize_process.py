import os
from pathlib import Path

from secfsdstools.c_automation.task_framework import TaskResultState
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.e_filter.joinedfiltering import StmtJoinedFilter
from secfsdstools.f_standardize.standardizing import StandardizedBag
from secfsdstools.g_pipelines.filter_process import postloadfilter
from secfsdstools.g_pipelines.standardize_process import StandardizeProcess

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"


def _create_subfolders(data_path: Path, root_path: Path):
    # creates the subfolders in the provided directory. 

    joined_input_bag = ZipCollector(
        datapaths=[str(data_path)],
        forms_filter=["10-K", "10-Q"],
        stmt_filter=["BS", "IS", "CF"],
        tag_filter=None,
        post_load_filter=postloadfilter).collect().join()

    for stmt in ["BS", "IS", "CF"]:
        (root_path / stmt).mkdir(parents=True)
        joined_input_bag[StmtJoinedFilter(stmts=[stmt])].save(target_path=str(root_path / stmt))


def test_standardizer_process_direct(tmp_path):
    # Tests the case when the root folder directly contains the BS, CF, and IS folders
    root_path = tmp_path / "root"
    target_path = tmp_path / "target"

    _create_subfolders(TESTDATA_PATH / "parquet_new" / "quarter" / "2021q1.zip", root_path)

    # simply test the whole process since it is mainly a simple abstraction on the Task itself
    process = StandardizeProcess(
        root_dir=str(root_path),
        target_dir=str(target_path)
    )

    process.process()

    assert len(process.results[TaskResultState.SUCCESS]) == 1

    std_bs_bag = StandardizedBag.load(str(target_path / "BS"))
    std_is_bag = StandardizedBag.load(str(target_path / "IS"))
    std_cf_bag = StandardizedBag.load(str(target_path / "CF"))

    assert len(std_bs_bag.result_df) == 5435
    assert len(std_is_bag.result_df) == 7802
    assert len(std_cf_bag.result_df) == 5387


def test_standardizer_process_subfolders(tmp_path):
    # Tests the case when the root folder contains several subfolders which contain
    # the BS, CF, and IS folders
    root_path = tmp_path / "root"
    target_path = tmp_path / "target"

    _create_subfolders(TESTDATA_PATH / "parquet_new" / "quarter" / "2010q1.zip", root_path / "q1")
    _create_subfolders(TESTDATA_PATH / "parquet_new" / "quarter" / "2010q2.zip", root_path / "q2")
    _create_subfolders(TESTDATA_PATH / "parquet_new" / "quarter" / "2010q3.zip", root_path / "q3")

    # simply test the whole process since it is mainly a simple abstraction on the Task itself
    process = StandardizeProcess(
        root_dir=str(root_path),
        target_dir=str(target_path)
    )

    process.process()

    assert len(process.results[TaskResultState.SUCCESS]) == 3

    std_bs_bag_q1 = StandardizedBag.load(str(target_path / "q1" / "BS"))
    std_is_bag_q1 = StandardizedBag.load(str(target_path / "q1" / "IS"))
    std_cf_bag_q1 = StandardizedBag.load(str(target_path / "q1" / "CF"))

    assert len(std_bs_bag_q1.result_df) == 458
    assert len(std_is_bag_q1.result_df) == 506
    assert len(std_cf_bag_q1.result_df) == 460

    std_bs_bag_q2 = StandardizedBag.load(str(target_path / "q2" / "BS"))
    std_is_bag_q2 = StandardizedBag.load(str(target_path / "q2" / "IS"))
    std_cf_bag_q2 = StandardizedBag.load(str(target_path / "q2" / "CF"))

    assert len(std_bs_bag_q2.result_df) == 457
    assert len(std_is_bag_q2.result_df) == 516
    assert len(std_cf_bag_q2.result_df) == 464
