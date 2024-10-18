import logging

from secfsdstools.x_examples.automation.filter_process import FilterProcess
from secfsdstools.x_examples.automation.automation import CombineProcess

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    import pathlib
    path = pathlib.Path("c:/data/sec/automated/_1_filtered/")
    results = list(path.glob("quarter/**/joined/BS"))
    print(results)

    # process = FilterProcess(parquet_dir="C:/data/sec/automated/parquet",
    #                         filtered_dir="C:/data/sec/automated/filter")
    #
    # process.process()

    process = CombineProcess(
        filtered_dir="C:/data/sec/automated/_1_filtered",
        bag_type="raw",
        execute_serial=True
    )

    process.process()
