"""
reading the data of a whole zip file.
"""
from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd

from secfsdstools.a_config.configmgt import Configuration, ConfigurationManager
from secfsdstools.a_utils.fileutils import read_df_from_file_in_zip
from secfsdstools.d_index.indexdataaccess import DBIndexingAccessor
from secfsdstools.e_read.basereportreading import BaseReportReader


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


class ZipReportReader(BaseReportReader):
    """
    ZipReport Reader, reads the data of a whole zip file
    """

    @classmethod
    def get_zip_by_name(cls, name: str, configuration: Optional[Configuration] = None):
        """
        creates a ZipReportReader instance for the given name of the zipfile.
        Args:
            name (str): name of the zipfile (without the path)
            configuration (Configuration, optional, None): configuration object
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        accessor = DBIndexingAccessor(configuration.db_dir)
        zipfilepath = accessor.read_index_file_for_filename(filename=name).fullPath
        return ZipReportReader(zipfile=zipfilepath)

    def __init__(self, zipfile: str):
        super().__init__()
        self.zipfile = zipfile

    def _read_df_from_raw(self, file_type: str) -> pd.DataFrame:
        """
        reads the raw data of a zipfile
        """
        return read_df_from_file_in_zip(zip_file=self.zipfile,
                                        file_to_extract=file_type)

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

        self._read_raw_data()  # lazy load the data if necessary
        num_entries = len(self.num_df)
        pre_entries = len(self.pre_df)
        number_of_reports = len(self.sub_df)
        reports_per_period_date: Dict[int, int] = self.sub_df.period.value_counts().to_dict()
        reports_per_form: Dict[str, int] = self.sub_df.form.value_counts().to_dict()

        return ZipFileStats(num_entries=num_entries,
                            pre_entries=pre_entries,
                            number_of_reports=number_of_reports,
                            reports_per_form=reports_per_form,
                            reports_per_period_date=reports_per_period_date
                            )
