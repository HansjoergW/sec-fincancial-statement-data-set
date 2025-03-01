import logging

from secfsdstools.a_utils.fileutils import get_directories_in_directory
from secfsdstools.f_standardize.standardizing import StandardizedBag
from secfsdstools.g_pipelines.standardize_process import StandardizeProcess

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    process = StandardizeProcess(root_dir="c:/data/sec/automated/_1_filtered_by_stmt_joined/quarter",
                                 target_dir="c:/data/sec/automated/_1_standardized/quarter",
                                  execute_serial=False
                                 )
    #process.process()


    available = get_directories_in_directory(process.target_dir)
    bags = [StandardizedBag.load(f"{process.target_dir}/{e}/BS") for e in available]
    total = StandardizedBag.concat(bags)
    total.save("c:/data/sec/automated/_1_standardized/all/BS")

