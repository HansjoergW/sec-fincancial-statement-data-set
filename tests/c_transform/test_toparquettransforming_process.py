import os
import shutil

import pandas as pd
from secfsdstools.c_transform.toparquettransforming_process import ToParquetTransformerProcess

CURRENT_DIR, _ = os.path.split(__file__)
ZIP_DIR = os.path.join(CURRENT_DIR, '../_testdata/zip')


def test_taskcreation(tmp_path):
    os.makedirs(tmp_path / 'quarter')
    transformer = ToParquetTransformerProcess(
        zip_dir=ZIP_DIR,
        parquet_dir=str(tmp_path),
        file_type='quarter',
        keep_zip_files=True
    )

    tasks = transformer.calculate_tasks()

    assert len(tasks) == 3

def test_transformation(tmp_path):
    os.makedirs(tmp_path / 'quarter')
    transformer = ToParquetTransformerProcess(
        zip_dir=ZIP_DIR,
        parquet_dir=str(tmp_path),
        file_type='quarter',
        keep_zip_files=True
    )

    transformer.process()

    sub_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'sub.txt.parquet')
    pre_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'pre.txt.parquet')
    num_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'num.txt.parquet')

    assert num_1_df.shape == (194741, 10)
    assert pre_1_df.shape == (64151, 10)
    assert sub_1_df.shape == (495, 36)

    sub_2_df = pd.read_parquet(tmp_path / 'quarter' / '2010q2.zip' / 'sub.txt.parquet')
    assert sub_2_df.shape == (522, 36)


def test_transformation_removefiles(tmp_path):
    zip_temp_dir = tmp_path / 'zip'
    os.makedirs(tmp_path / 'quarter')

    shutil.copytree(ZIP_DIR, zip_temp_dir)

    transformer = ToParquetTransformerProcess(
        zip_dir=str(zip_temp_dir),
        parquet_dir=str(tmp_path),
        file_type='quarter',
        keep_zip_files=False
    )

    transformer.process()

    sub_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'sub.txt.parquet')
    pre_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'pre.txt.parquet')
    num_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'num.txt.parquet')

    assert num_1_df.shape == (194741, 10)
    assert pre_1_df.shape == (64151, 10)
    assert sub_1_df.shape == (495, 36)

    sub_2_df = pd.read_parquet(tmp_path / 'quarter' / '2010q2.zip' / 'sub.txt.parquet')
    assert sub_2_df.shape == (522, 36)

    # check if file is deleted
    files_in_zip_temp_dir = os.listdir(zip_temp_dir)
    assert len(files_in_zip_temp_dir) == 0
