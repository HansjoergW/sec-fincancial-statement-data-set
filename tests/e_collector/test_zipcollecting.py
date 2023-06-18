import os
from unittest.mock import patch

import pandas as pd
import pytest

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexFileProcessingState
from secfsdstools.e_collector.zipcollecting import ZipCollector

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = f'{CURRENT_DIR}/../_testdata/parquet/quarter/2010q1.zip'


@pytest.fixture
def zipcollector():
    zipcollector = ZipCollector(datapath=PATH_TO_ZIP)
    zipcollector.collect()
    return zipcollector


def test_cm_get_zip_by_name():
    instance = IndexFileProcessingState(fileName="", status="", entries=0, processTime="",
                                        fullPath=PATH_TO_ZIP)

    with patch(
            "secfsdstools.c_index.indexdataaccess.ParquetDBIndexingAccessor.read_index_file_for_filename",
            return_value=instance):
        zipcollector = ZipCollector.get_zip_by_name(name="2010q1.zip",
                                                    configuration=Configuration(db_dir="",
                                                                                download_dir="",
                                                                                user_agent_email="",
                                                                                parquet_dir=""))
        assert zipcollector.collect().num_df.shape == (151692, 9)


def test_read_raw_data(zipcollector):
    assert zipcollector.collect().num_df.shape == (151692, 9)
    assert zipcollector.collect().pre_df.shape == (88378, 10)
    assert zipcollector.collect().sub_df.shape == (495, 36)


def test_forms_filter():
    zipcollector = ZipCollector(datapath=PATH_TO_ZIP, forms_filter=['10-K'])
    bag = zipcollector.collect()

    merged_pre_df = pd.merge(bag.sub_df[['adsh', 'form']], bag.pre_df, on=['adsh'])
    assert merged_pre_df.form.unique().tolist() == ['10-K']

    merged_num_df = pd.merge(bag.sub_df[['adsh', 'form']], bag.num_df, on=['adsh'])
    assert merged_num_df.form.unique().tolist() == ['10-K']


def test_stmt_filter():
    zipcollector = ZipCollector(datapath=PATH_TO_ZIP, stmt_filter=['BS'])
    bag = zipcollector.collect()

    assert bag.pre_df.stmt.unique().tolist() == ['BS']

def test_tag_filter():
    zipcollector = ZipCollector(datapath=PATH_TO_ZIP, tag_filter=['Assets'])
    bag = zipcollector.collect()

    assert bag.pre_df.tag.unique().tolist() == ['Assets']
    assert bag.num_df.tag.unique().tolist() == ['Assets']
