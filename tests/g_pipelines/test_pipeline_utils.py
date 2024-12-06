import os
from pathlib import Path
from typing import List

from secfsdstools.d_container.databagmodel import JoinedDataBag

from secfsdstools.g_pipelines.pipeline_utils import concat_bags

CURRENT_DIR, _ = os.path.split(__file__)
TESTDATA_PATH = Path(CURRENT_DIR) / ".." / "_testdata"


def test_concat_bags(tmp_path):
    parts: List[Path] = [
        TESTDATA_PATH / "joined" / "2010q1.zip",
        TESTDATA_PATH / "joined" / "2010q2.zip",
    ]

    concat_bags(paths_to_concat=parts, target_path=tmp_path)

    bag = JoinedDataBag.load(target_path=str(tmp_path))
    # simply check if data were successful stored and loaded
    print(bag.sub_df.shape)
    assert len(bag.sub_df) > 0
