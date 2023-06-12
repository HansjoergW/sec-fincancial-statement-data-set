"""
loads all the data from one single zip file, resp. the folder with the three parquet files to
which the zip file was transformed to.
"""
import os
from dataclasses import dataclass
from typing import Optional, Dict, List

import pandas as pd

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.a_utils.constants import NUM_TXT, PRE_TXT, SUB_TXT
from secfsdstools.c_index.indexdataaccess import ParquetDBIndexingAccessor
from secfsdstools.d_container.databagmodel import RawDataBag


@dataclass
class ZipFileStats:
    """
    Contains simple statistics of a report.
    """
    num_entries: int
    pre_entries: int
    number_of_reports: int
    reports_per_form: Dict[str, int]
    reports_per_period_date: Dict[int, int]


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

    def _read_df_from_raw_parquet(self,
                                  file: str,
                                  filters=None) -> pd.DataFrame:
        return pd.read_parquet(os.path.join(self.datapath, f'{file}.parquet'),
                               filters=filters)

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
            pre_filter = []
            num_filter = []

            if self.forms_filter:
                sub_filter.append(('form', 'in', self.forms_filter))

            sub_df = self._read_df_from_raw_parquet(
                file=SUB_TXT, filters=sub_filter if sub_filter else None)

            if self.forms_filter:
                adshs = sub_df.adsh.to_list()
                adsh_filter_expression = ('adsh', 'in', adshs)
                pre_filter.append(adsh_filter_expression)
                num_filter.append(adsh_filter_expression)

            if self.stmt_filter:
                pre_filter.append(('stmt', 'in', self.stmt_filter))

            if self.tag_filter:
                tag_filter_expression = ('tag', 'in', self.tag_filter)
                pre_filter.append(tag_filter_expression)
                num_filter.append(tag_filter_expression)

            pre_df = self._read_df_from_raw_parquet(
                file=PRE_TXT, filters=pre_filter if pre_filter else None
            )

            num_df = self._read_df_from_raw_parquet(
                file=NUM_TXT, filters=num_filter if num_filter else None
            )

            self.databag = RawDataBag.create(sub_df=sub_df, pre_df=pre_df, num_df=num_df)
        return self.databag

    def statistics(self) -> ZipFileStats:
        """
        calculate a few simple statistics of a report.
        - number of entries in the num-file
        - number of entries in the pre-file
        - number of reports in the zip-file (equals number of entries in sub-file)
        - number of reports per form (10-K, 10-Q, ...)
        - number of reports per period date (counts per value in the period column of sub-file)

        Rreturns:
            ZipFileStats: instance with basic report infos
        """

        databag = self.collect()
        num_entries = len(databag.num_df)
        pre_entries = len(databag.pre_df)
        number_of_reports = len(databag.sub_df)
        reports_per_period_date: Dict[int, int] = databag.sub_df.period.value_counts().to_dict()
        reports_per_form: Dict[str, int] = databag.sub_df.form.value_counts().to_dict()

        return ZipFileStats(num_entries=num_entries,
                            pre_entries=pre_entries,
                            number_of_reports=number_of_reports,
                            reports_per_form=reports_per_form,
                            reports_per_period_date=reports_per_period_date
                            )
