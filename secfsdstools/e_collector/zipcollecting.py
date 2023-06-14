"""
loads all the data from one single zip file, resp. the folder with the three parquet files to
which the zip file was transformed to.
"""
from typing import Optional, List

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.constants import NUM_TXT, PRE_TXT, SUB_TXT
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
        super().__init__()
        self.datapath = datapath
        self.databag: Optional[RawDataBag] = None
        self.forms_filter = forms_filter
        self.stmt_filter = stmt_filter
        self.tag_filter = tag_filter

    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag

        Returns:
            RawDataBag: the collected Data
        """
        if self.databag is None:
            # bsp chat gpt
            # filter_expression = pd.Series(['column_name']).isin(accepted_values)
            # df = pd.read_parquet('data.parquet', filters={'column_name': filter_expression})

            sub_filter = []

            if self.forms_filter:
                sub_filter.append(('form', 'in', self.forms_filter))

            sub_df = self._read_df_from_raw_parquet(
                file=SUB_TXT, path=self.datapath, filters=sub_filter if sub_filter else None)

            adshs = sub_df.adsh.to_list() if self.forms_filter else None

            pre_filter, num_filter = self._get_pre_num_filters(adshs=adshs,
                                                               stmts=self.stmt_filter,
                                                               tags=self.tag_filter)

            pre_df = self._read_df_from_raw_parquet(
                file=PRE_TXT, path=self.datapath, filters=pre_filter if pre_filter else None
            )

            num_df = self._read_df_from_raw_parquet(
                file=NUM_TXT, path=self.datapath, filters=num_filter if num_filter else None
            )

            self.databag = RawDataBag.create(sub_df=sub_df, pre_df=pre_df, num_df=num_df)

        return self.databag
