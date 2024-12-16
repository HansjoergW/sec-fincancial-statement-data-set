import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector
from secfsdstools.g_pipelines.filter_process import FilterTask, postloadfilter

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"


def test_filtertask(tmp_path):
    raw_bag: RawDataBag = RawDataBag.load(str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip"))

    # check expected unfiltered counts
    assert len(raw_bag.num_df) == 151_692
    assert len(raw_bag.pre_df) == 88_378

    return_value = ZipCollector(datapaths=[str(TESTDATA_PATH / "parquet" / "quarter" / "2010q1.zip")],
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

