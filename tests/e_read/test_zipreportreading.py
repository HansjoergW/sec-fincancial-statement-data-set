import os
from unittest.mock import patch

import pytest

from secfsdstools.a_config.configmgt import Configuration
from secfsdstools.d_index.indexdataaccess import IndexFileProcessingState
from secfsdstools.e_read.zipreportreading import ZipReportReader

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = CURRENT_DIR + '/testdata/2010q1.zip'


@pytest.fixture
def zipreader():
    zipreader = ZipReportReader(zipfile=PATH_TO_ZIP)
    zipreader._read_raw_data()
    return zipreader


def test_read_raw_data(zipreader):
    assert zipreader.get_raw_num_data().shape == (151692, 9)
    assert zipreader.get_raw_pre_data().shape == (88378, 10)
    assert zipreader.get_raw_sub_data().shape == (495, 36)


def test_financial_statements_for_tags(zipreader):
    # read only for the actual period
    fin_stmts_df = zipreader.financial_statements_for_period()

    assert fin_stmts_df.shape == (61245, 18)

    # read for the actual period and the previous period
    fin_stmts_df = zipreader.financial_statements_for_period_and_previous_period()

    assert fin_stmts_df.shape == (62255, 25)

    # read only for the actual period
    fin_stmts_df = zipreader.financial_statements_for_period(tags=['Assets'])
    assert fin_stmts_df.shape == (513, 18)


def test_statistics(zipreader):
    stats = zipreader.statistics()

    assert stats.num_entries == 151692
    assert stats.pre_entries == 88378
    assert stats.number_of_reports == 495
    assert stats.reports_per_form['10-Q'] == 80
    assert stats.reports_per_period_date[20091231] == 434


def test_cm_get_zip_by_name():
    instance = IndexFileProcessingState(fileName="", status="", entries=0, processTime="",
                                        fullPath=PATH_TO_ZIP)

    with patch("secfsdstools.d_index.indexdataaccess.DBIndexingAccessor.read_index_file_for_filename",
               return_value=instance):
        zipreader = ZipReportReader.get_zip_by_name(name="2010q1.zip",
                                                    configuration=Configuration(db_dir="",
                                                                                download_dir="",
                                                                                user_agent_email=""))
        zipreader._read_raw_data()
        assert zipreader.get_raw_num_data().shape == (151692, 9)

