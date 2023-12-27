"""
Contains basic logic that produces separated databags for every of the main financial statements (BS, CF, IS)
containing data from all available zip files.

The same logic is also contained and further explained in the 06_bulk_data_processing_deep_dive.ipynb notebook.
Please have a look at this notebook for a detailed explanation of the logic
"""
import os
from glob import glob
from typing import Callable, Optional
from typing import List

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag
from secfsdstools.e_collector.zipcollecting import ZipCollector


def default_postloadfilter(databag: RawDataBag) -> RawDataBag:
    """
    defines a default post filter method that can be used ba ZipCollectors.
    It combines the filters:
        ReportPeriodRawFilter, MainCoregRawFilter, OfficialTagsOnlyRawFilter, USDOnlyRawFilter
    """
    from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, \
        OfficialTagsOnlyRawFilter, USDOnlyRawFilter

    return databag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][
        USDOnlyRawFilter()]


def save_databag(databag: RawDataBag, base_path: str, sub_path: str) -> JoinedDataBag:
    """
    helper method to save the RawDataBag and the joined version of it under a certain base_path
    and sub_path.

    the target path for the rawdatabag is <base_path>/<sub_path>/raw.
    the target path for the joineddatabag is <base_path>/<sub_path>/joined.

    Args:
        databag: databag to be saved
        base_path: base path under which the data will be stored
        sub_path: sub path under which the data will be stored

    Returns:
        JoinedDataBag: the joined databag that was created during the save process
    """

    target_path_raw = os.path.join(base_path, sub_path, 'raw')
    print(f"store rawdatabag under {target_path_raw}")
    os.makedirs(target_path_raw, exist_ok=True)
    databag.save(target_path_raw)

    target_path_joined = os.path.join(base_path, sub_path, 'joined')
    os.makedirs(target_path_joined, exist_ok=True)
    print("create joined databag")
    joined_databag = databag.join()

    print(f"store joineddatabag under {target_path_joined}")
    joined_databag.save(target_path_joined)
    return joined_databag


def load_all_financial_statements_parallel(
        financial_statement: str,
        post_load_filter: Optional[Callable[[RawDataBag], RawDataBag]] = None) -> RawDataBag:
    """
    loads the data for a certain statement (e.g. BS, CF, IS, ...) from all availalbe zip files
    and returns a single RawDataBag with all information.
    it filters for 10-K and 10-Q reports.

    Args:
        financial_statement (str): the statement you want to read the data for "BS", "CF", "IS"
        post_load_filter (Callable, optional): a post_load_filter method that is applied after
         loading of every zip file

    Returns:
        RawDataBag: the databag with the read data

    """

    collector: ZipCollector = ZipCollector.get_all_zips(forms_filter=["10-K", "10-Q"],
                                                        stmt_filter=[financial_statement],
                                                        post_load_filter=post_load_filter)
    return collector.collect()


def create_datasets_for_main_statements_parallel(base_path: str = "./set/parallel/"):
    """
    Creates the raw and joined datasets for all the three main statements: BS, CF, IS.

    The data from the different zip files are loaded in parallel. Therefore, about 16GB of free memory is needed.

    the created folder hiearchy looks as follows:
    <pre>
        - <base_path>
          - BS
            - raw
            - joined
          - CF
            - raw
            - joined
          - IS
            - raw
            - joined
    </pre>
    """

    for statement_to_load in ["BS", "CF", "IS"]:
        print("load data for ", statement_to_load)
        rawdatabag = load_all_financial_statements_parallel(
            financial_statement=statement_to_load,
            post_load_filter=default_postloadfilter
        )
        save_databag(databag=rawdatabag, base_path=base_path, sub_path=statement_to_load)


def read_all_zip_names() -> List[str]:
    configuration = ConfigurationManager.read_config_file()
    dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)

    # exclude 2009q1.zip, since this is empty and causes an error when it is read with a filter
    return [x.fileName for x in dbaccessor.read_all_indexfileprocessing() if
            not x.fullPath.endswith("2009q1.zip")]


def build_tmp_set(financial_statement: str,
                  file_names: List[str],
                  base_path: str = "set/tmp/",
                  post_load_filter: Optional[Callable[[RawDataBag], RawDataBag]] = None):
    """
    This function reads the data in sequence from the provided list of zip file names.
    It filters according to the defined financial_statement and stores the data in
    specific subfolders.

    the folder structure will look like
    <target_path>/<file_name>/<financial_statement>/raw
    <target_path>/<file_name>/<financial_statement>/joined

    Args:
        financial_statement (str): the statement you want to read the data for "BS", "CF", "IS"
        post_load_filter (Callable, optional): a post_load_filter method that is applied after
         loading of every zip file
        file_names (List[str]): List with the filenames to be processed
        base_path (str): base_path under which the process data is saved.
    """

    for file_name in file_names:
        collector = ZipCollector.get_zip_by_name(name=file_name,
                                                 forms_filter=["10-K", "10-Q"],
                                                 stmt_filter=[financial_statement],
                                                 post_load_filter=post_load_filter)

        rawdatabag = collector.collect()

        target_path = os.path.join(base_path, file_name)
        # saving the raw databag, joining and saving the joined databag
        save_databag(databag=rawdatabag, base_path=target_path, sub_path=financial_statement)


def create_rawdatabag(financial_statement: str,
                      tmp_path: str = "set/tmp/",
                      target_path: str = "set/serial/"):
    """
    Concatenates the preprocessed and by statement separated rawdatabags into a single databag.

    Args:
        financial_statement: the statement for which data has to be concatenated.
        tmp_path: the path where the temporary files are stored
        target_path: the target path of the daset
    """
    raw_files = glob(f"{tmp_path}/*/{financial_statement}/raw/", recursive=True)
    raw_databags = [RawDataBag.load(file) for file in raw_files]
    raw_databag = RawDataBag.concat(raw_databags)
    target_path_raw = os.path.join(target_path, financial_statement, 'raw')
    print(f"store rawdatabag under {target_path_raw}")
    os.makedirs(target_path_raw, exist_ok=True)
    raw_databag.save(target_path_raw)


def create_joineddatabag(financial_statement: str,
                         tmp_path: str = "set/tmp/",
                         target_path: str = "set/serial/"):
    """
    Concatenates the preprocessed and by statement separated joineddatabag into a single databag.

    Args:
        financial_statement: the statement for which data has to be concatenated.
        tmp_path: the path where the temporary files are stored
        target_path: the target path of the daset
    """

    joined_files = glob(f"{tmp_path}/*/{financial_statement}/joined/", recursive=True)
    joined_databags = [JoinedDataBag.load(file) for file in joined_files]
    joined_databag = JoinedDataBag.concat(joined_databags)
    target_path_joined = os.path.join(target_path, financial_statement, 'joined')
    print(f"store joineddatabag under {target_path_joined}")
    os.makedirs(target_path_joined, exist_ok=True)
    joined_databag.save(target_path_joined)


def create_datasets_for_main_statements_serial(target_path: str = "set/parallel/",
                                               tmp_path: str = "set/tmp"):
    """
    Creates the rawdatabag and joineddatabag for all three main financial statements (BS, CF, IS).

    It is done in serial manner, and therefore needs less resources than the parallel approach.

    the created folder hiearchy looks as follows:
    <pre>
        - <target_path>
          - BS
            - raw
            - joined
          - CF
            - raw
            - joined
          - IS
            - raw
            - joined
    </pre>
    """
    file_names = read_all_zip_names()

    # create the temporary datasets
    # Note: calling the build_tmp_set doesn't needs a lot of memory
    build_tmp_set(financial_statement="BS", file_names=file_names,
                  post_load_filter=default_postloadfilter, base_path=tmp_path)
    build_tmp_set(financial_statement="IS", file_names=file_names,
                  post_load_filter=default_postloadfilter, base_path=tmp_path)
    build_tmp_set(financial_statement="CF", file_names=file_names,
                  post_load_filter=default_postloadfilter, base_path=tmp_path)

    # Note: calling the create_rawdatabag needs about 8-10 GB of free memory.
    #       the memory should be garbage collected between the different create_rawdatabag calls
    create_rawdatabag(financial_statement="BS", target_path=target_path, tmp_path=tmp_path)
    create_rawdatabag(financial_statement="IS", target_path=target_path, tmp_path=tmp_path)
    create_rawdatabag(financial_statement="CF", target_path=target_path, tmp_path=tmp_path)

    # Note: calling the create_joinedatabag needs about 4 GB of free memory.
    #       the memory should be garbage collected between the different create_joineddatabag calls
    create_joineddatabag(financial_statement="BS", target_path=target_path, tmp_path=tmp_path)
    create_joineddatabag(financial_statement="IS", target_path=target_path, tmp_path=tmp_path)
    create_joineddatabag(financial_statement="CF", target_path=target_path, tmp_path=tmp_path)


if __name__ == '__main__':
    print("depending on your hardware resources, run the parallel or the serial logic.",
          "Just uncommented the desired line..")
    create_datasets_for_main_statements_parallel()
    # create_datasets_for_main_statements_serial()
