"""
loads all the data from one single zip file, resp. the folder with the three parquet files to
which the zip file was transformed to.
"""
from typing import Optional, List

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.parallelexecution import ParallelExecutor
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_collector.basecollector import BaseCollector


class ZipCollector:
    """
    Reads all the data from a single zip file, resp. the folder containing the data in the
    parquet format to which the zip file was transformed into.
    """

    @classmethod
    def get_zip_by_name(cls,
                        name: str,
                        forms_filter: Optional[List[str]] = None,
                        stmt_filter: Optional[List[str]] = None,
                        tag_filter: Optional[List[str]] = None,
                        configuration: Optional[Configuration] = None):
        """
        creates a ZipReportReader instance for the given name of the zipfile.
        Args:
            name (str): name of the zipfile (without the path)

            forms_filter (List[str], optional, None):
                List of forms that should be read (10-K, 10-Q, ...)

            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)

            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)

            configuration (Configuration, optional, None): configuration object
        """
        return cls.get_zip_by_names(names=[name],
                                    forms_filter=forms_filter,
                                    stmt_filter=stmt_filter,
                                    tag_filter=tag_filter,
                                    configuration=configuration)

    @classmethod
    def get_zip_by_names(cls,
                         names: List[str],
                         forms_filter: Optional[List[str]] = None,
                         stmt_filter: Optional[List[str]] = None,
                         tag_filter: Optional[List[str]] = None,
                         configuration: Optional[Configuration] = None):
        """
        creates a ZipReportReader instance for the given names of the zipfiles.
        Args:
            names (List[str]): names of the zipfiles (without the path)

            forms_filter (List[str], optional, None):
                List of forms that should be read (10-K, 10-Q, ...)

            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)

            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)

            configuration (Configuration, optional, None): configuration object
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)

        datapaths = [x.fullPath for x in dbaccessor.read_index_files_for_filenames(filenames=names)]
        return ZipCollector(datapaths=datapaths,
                            forms_filter=forms_filter,
                            stmt_filter=stmt_filter,
                            tag_filter=tag_filter)

    @classmethod
    def get_all_zips(cls,
                     forms_filter: Optional[List[str]] = None,
                     stmt_filter: Optional[List[str]] = None,
                     tag_filter: Optional[List[str]] = None,
                     configuration: Optional[Configuration] = None):
        """
        ATTENTION: this will take some time since data from all zip files are read at once.
        Moreover, if you don't apply directly filters, it will load a load of data.

        Creates a ZipReportReader that gets data from all available zipfiles.
        Args:
            names (List[str]): names of the zipfiles (without the path)

            forms_filter (List[str], optional, None):
                List of forms that should be read (10-K, 10-Q, ...)

            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)

            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)

            configuration (Configuration, optional, None): configuration object
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)

        datapaths = [x.fullPath for x in dbaccessor.read_all_indexfileprocessing()]
        return ZipCollector(datapaths=datapaths,
                            forms_filter=forms_filter,
                            stmt_filter=stmt_filter,
                            tag_filter=tag_filter)

    def __init__(self,
                 datapaths: List[str],
                 forms_filter: Optional[List[str]] = None,
                 stmt_filter: Optional[List[str]] = None,
                 tag_filter: Optional[List[str]] = None):

        self.datapaths = datapaths
        self.forms_filter = forms_filter
        self.stmt_filter = stmt_filter
        self.tag_filter = tag_filter

    def _multi_zipcollect(self) -> RawDataBag:

        datapaths: List[str] = self.datapaths

        def get_entries() -> List[str]:
            return datapaths

        def process_element(datapath: str) -> RawDataBag:
            print(str)
            collector = BaseCollector(datapath=datapath,
                                      stmt_filter=self.stmt_filter,
                                      tag_filter=self.tag_filter)

            sub_filter = ('form', 'in', self.forms_filter) if self.forms_filter else None

            return collector.basecollect(sub_df_filter=sub_filter)

        def post_process(parts: List[RawDataBag]) -> List[RawDataBag]:
            # do nothing
            return parts

        execute_serial = False
        # no need for parallel execution if there is just one zipfile to load
        if len(self.datapaths) == 1:
            execute_serial = True
        executor = ParallelExecutor(chunksize=0, execute_serial=execute_serial)

        executor.set_get_entries_function(get_entries)
        executor.set_process_element_function(process_element)
        executor.set_post_process_chunk_function(post_process)

        # we ignore the missing, since get_entries always returns the whole list
        collected_reports: List[RawDataBag]
        collected_reports, _ = executor.execute()

        return RawDataBag.concat(collected_reports)

    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag

        Returns:
            RawDataBag: the collected Data
        """
        return self._multi_zipcollect()
