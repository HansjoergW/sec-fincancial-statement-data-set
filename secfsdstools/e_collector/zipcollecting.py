"""
loads all the data from one single zip file, resp. the folder with the three parquet files to
which the zip file was transformed to.
"""
from typing import Optional, List

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.e_collector.basecollector import BaseCollector


class ZipCollector(BaseCollector):
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
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)

        datapath = dbaccessor.read_index_file_for_filename(filename=name).fullPath
        return ZipCollector(datapath=datapath,
                            forms_filter=forms_filter,
                            stmt_filter=stmt_filter,
                            tag_filter=tag_filter)

    def __init__(self,
                 datapath: str,
                 forms_filter: Optional[List[str]] = None,
                 stmt_filter: Optional[List[str]] = None,
                 tag_filter: Optional[List[str]] = None):
        super().__init__(datapath=datapath, stmt_filter=stmt_filter, tag_filter=tag_filter)
        self.datapath = datapath
        self.databag: Optional[RawDataBag] = None
        self.forms_filter = forms_filter

    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag

        Returns:
            RawDataBag: the collected Data
        """

        sub_filter = ('form', 'in', self.forms_filter) if self.forms_filter else None
        return self._collect(sub_df_filter=sub_filter)
