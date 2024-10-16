import logging

from secfsdstools.x_examples.automation.filter_process import FilterProcess

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
