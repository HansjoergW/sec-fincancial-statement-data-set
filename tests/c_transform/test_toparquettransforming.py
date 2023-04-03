import os
import pandas as pd

from secfsdstools.c_transform.toparquettransforming import ToParquetTransformer

CURRENT_DIR, _ = os.path.split(__file__)
ZIP_DIR = os.path.join(CURRENT_DIR, 'testdata')


def test_transformation(tmp_path):
    os.makedirs(tmp_path / 'quarter')
    transformer = ToParquetTransformer(
        zip_dir=ZIP_DIR,
        parquet_dir=str(tmp_path),
        file_type='quarter'
    )

    transformer.process()

    sub_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'sub.txt.parquet')
    pre_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'pre.txt.parquet')
    num_1_df = pd.read_parquet(tmp_path / 'quarter' / '2010q1.zip' / 'num.txt.parquet')

    assert num_1_df.shape == (151692, 9)
    assert pre_1_df.shape == (88378, 10)
    assert sub_1_df.shape == (495, 36)

    sub_2_df = pd.read_parquet(tmp_path / 'quarter' / '2010q2.zip' / 'sub.txt.parquet')
    assert sub_2_df.shape == (522, 36)


