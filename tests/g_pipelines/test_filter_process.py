import os
from pathlib import Path
from unittest.mock import patch

from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.g_pipelines.filter_process import FilterTask, postloadfilter, ByStmtFilterTask, \
    FilterProcess

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"


def test_filtertask_raw(tmp_path):
    raw_bag: RawDataBag = RawDataBag.load(str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip"))

    # check expected unfiltered counts
    assert len(raw_bag.num_df) == 151_692
    assert len(raw_bag.pre_df) == 88_378

    return_value = ZipCollector(
        datapaths=[str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip")],
        forms_filter=["10-K", "10-Q"],
        stmt_filter=["BS", "IS"],
        tag_filter=None,
        post_load_filter=postloadfilter)

    with patch.object(ZipCollector, "get_zip_by_name", return_value=return_value):
        task = FilterTask(
            zip_file_name="2010q1.zip",
            target_path=tmp_path / "result",
            bag_type="raw",
            stmts=["BS", "IS"]
        )

        task.prepare()
        task.execute()
        result = task.commit()

        assert result == "success"

        fitlered_bag: RawDataBag = RawDataBag.load(str(tmp_path / "result"))

        assert len(fitlered_bag.num_df) == 52_542
        assert len(fitlered_bag.pre_df) == 39_523


def test_filtertask_joined(tmp_path):
    raw_bag: RawDataBag = RawDataBag.load(str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip"))

    # check expected unfiltered counts
    assert len(raw_bag.num_df) == 151_692
    assert len(raw_bag.pre_df) == 88_378

    return_value = ZipCollector(
        datapaths=[str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip")],
        forms_filter=["10-K", "10-Q"],
        stmt_filter=["BS", "IS"],
        tag_filter=None,
        post_load_filter=postloadfilter)

    with patch.object(ZipCollector, "get_zip_by_name", return_value=return_value):
        task = FilterTask(
            zip_file_name="2010q1.zip",
            target_path=tmp_path / "result",
            bag_type="joined",
            stmts=["BS", "IS"]
        )

        task.prepare()
        task.execute()
        result = task.commit()

        assert result == "success"

        fitlered_bag: JoinedDataBag = JoinedDataBag.load(str(tmp_path / "result"))

        assert len(fitlered_bag.pre_num_df) == 31_253


def test_bystmtfiltertask_raw(tmp_path):
    raw_bag: RawDataBag = RawDataBag.load(str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip"))

    # check expected unfiltered counts
    assert len(raw_bag.num_df) == 151_692
    assert len(raw_bag.pre_df) == 88_378

    return_value = ZipCollector(
        datapaths=[str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip")],
        forms_filter=["10-K", "10-Q"],
        stmt_filter=["BS", "IS"],
        tag_filter=None,
        post_load_filter=postloadfilter)

    with patch.object(ZipCollector, "get_zip_by_name", return_value=return_value):
        task = ByStmtFilterTask(
            zip_file_name="2010q1.zip",
            target_path=tmp_path / "result",
            bag_type="raw",
            stmts=["BS", "IS"]
        )

        task.prepare()
        task.execute()
        result = task.commit()

        assert result == "success"

        fitlered_bag_bs: RawDataBag = RawDataBag.load(str(tmp_path / "result" / "BS"))
        fitlered_bag_is: RawDataBag = RawDataBag.load(str(tmp_path / "result" / "IS"))

        assert len(fitlered_bag_bs.num_df) == 52_542
        assert len(fitlered_bag_bs.pre_df) == 24_411
        assert len(fitlered_bag_is.num_df) == 52_542
        assert len(fitlered_bag_is.pre_df) == 15_112


def test_bystmtfiltertask_joined(tmp_path):
    raw_bag: RawDataBag = RawDataBag.load(str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip"))

    # check expected unfiltered counts
    assert len(raw_bag.num_df) == 151_692
    assert len(raw_bag.pre_df) == 88_378

    return_value = ZipCollector(
        datapaths=[str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip")],
        forms_filter=["10-K", "10-Q"],
        stmt_filter=["BS", "IS"],
        tag_filter=None,
        post_load_filter=postloadfilter)

    with patch.object(ZipCollector, "get_zip_by_name", return_value=return_value):
        task = ByStmtFilterTask(
            zip_file_name="2010q1.zip",
            target_path=tmp_path / "result",
            bag_type="joined",
            stmts=["BS", "IS"]
        )

        task.prepare()
        task.execute()
        result = task.commit()

        assert result == "success"

        fitlered_bag_bs: JoinedDataBag = JoinedDataBag.load(str(tmp_path / "result" / "BS"))
        fitlered_bag_is: JoinedDataBag = JoinedDataBag.load(str(tmp_path / "result" / "IS"))

        assert len(fitlered_bag_bs.pre_num_df) == 18_557
        assert len(fitlered_bag_is.pre_num_df) == 12_696


def test_filterprocess(tmp_path):
    target_path: Path = tmp_path / "target"
    process = FilterProcess(
        bag_type="raw",
        db_dir=".",
        target_dir=str(target_path),
    )

    with patch.object(ParquetDBIndexingAccessor, "read_filenames_by_type", return_value=[]):
        tasks = process.calculate_tasks()
        assert len(tasks) == 0

    with patch.object(ParquetDBIndexingAccessor, "read_filenames_by_type",
                      return_value=["aname.zip"]):
        tasks = process.calculate_tasks()
        assert len(tasks) == 1

    (target_path / "quarter" / "2010q1.zip").mkdir(parents=True)
    with patch.object(ParquetDBIndexingAccessor, "read_filenames_by_type",
                      return_value=["2010q1.zip"]):
        tasks = process.calculate_tasks()
        assert len(tasks) == 0

    with patch.object(ParquetDBIndexingAccessor, "read_filenames_by_type",
                      return_value=["2010q1.zip", "2010q2.zip"]):
        tasks = process.calculate_tasks()
        assert len(tasks) == 1
