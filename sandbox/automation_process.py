import logging

from secfsdstools.x_examples.automation.automation import FilterProcess

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    process = FilterProcess(parquet_dir="C:/data/sec/automated/parquet",
                            filtered_dir="C:/data/sec/automated/filter")

    process.process()
