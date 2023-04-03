import logging

from secfsdstools.c_transform.toparquettransforming import ToParquetTransformer

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    zip_dir = "../data/dld"
    parquet_dir = "../data/parquet"

    transformer = ToParquetTransformer(parquet_dir=parquet_dir, zip_dir=zip_dir, file_type="quarter")
    transformer.process()
