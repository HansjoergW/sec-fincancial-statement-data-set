import os
from pathlib import Path

from secfsdstools.f_standardize.standardizing import StandardizedBag

from secfsdstools.c_automation.task_framework import AbstractProcess
from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_filter.joinedfiltering import StmtJoinedFilter
from secfsdstools.g_pipelines.filter_process import postloadfilter
from secfsdstools.g_pipelines.standardize_process import StandardizerTask

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"


def test_standardizer_task(tmp_path):
    root_path = tmp_path / "root"
    target_path = tmp_path / "target"

    hier muss 10-K und 10-Q auch gefiltert werden..
    besser mit dem ZipCollector laden, wie auch schon bei anderen test..

    return_value = ZipCollector(
        datapaths=[str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip")],
        forms_filter=["10-K", "10-Q"],
        stmt_filter=["BS", "IS"],
        tag_filter=None,
        post_load_filter=postloadfilter)

    # prepare test data
    raw_test_bag = RawDataBag.load(str(TESTDATA_PATH / "parquet" / "quarter" / "2021q1.zip"))
    raw_test_bag.num_df.loc[raw_test_bag.num_df.coreg.isna(), 'coreg'] = ''
    raw_test_bag = postloadfilter(raw_test_bag)
    joined_input_bag = raw_test_bag.join()

    for stmt in ["BS", "IS", "CF"]:
        (root_path / stmt).mkdir(parents=True)
        joined_input_bag[StmtJoinedFilter(stmts=[stmt])].save(target_path=str(root_path / stmt))

    task = StandardizerTask(
        root_path=root_path,
        target_path=target_path
    )

    result = AbstractProcess.process_task(task)

    assert result.result == "success"

    std_bs_bag = StandardizedBag.load(str(target_path / "BS"))
    std_is_bag = StandardizedBag.load(str(target_path / "IS"))
    std_cf_bag = StandardizedBag.load(str(target_path / "CF"))

