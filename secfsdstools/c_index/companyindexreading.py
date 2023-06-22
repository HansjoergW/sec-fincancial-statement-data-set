"""
Reads company information.
"""
import os
from typing import Dict, Optional, List

import pandas as pd

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexReport, ParquetDBIndexingAccessor
from secfsdstools.a_utils.constants import SUB_TXT


class CompanyIndexReader:
    """
    reads information for a single company
    """

    @classmethod
    def get_company_index_reader(cls, cik: int, configuration: Optional[Configuration] = None):
        """
        creates a company instance for the provided cik. If no  configuration object is passed,
        it reads the configuration from the config file.

        Args:
            cik (int): the central identification key which is assigned by the sec for every company
            configuration (Configuration, optional, None): Optional configuration object

        Returns:
            CompanyIndexReader: instance of Company Reader
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()
        dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)
        return CompanyIndexReader(cik, dbaccessor=dbaccessor)

    def __init__(self, cik: int, dbaccessor: ParquetDBIndexingAccessor):
        self.cik = cik
        self.dbaccessor = dbaccessor

    def get_latest_company_filing(self) -> Dict[str, str]:
        """
        returns the latest company information (the content in the sub.txt file)
        from the quarter-zip files.
        Returns:
            Dict[str, str]: dict with the information of the latest
             report as present in the sub.txt file.
        """
        latest_report = self.dbaccessor.find_latest_company_report(self.cik)
        return self._get_latest_company_filing_parquet(latest_report)

    def _get_latest_company_filing_parquet(self, latest_report: IndexReport) -> Dict[str, str]:
        latest_filing = pd.read_parquet(os.path.join(latest_report.fullPath, f'{SUB_TXT}.parquet'),
                                        filters=[('adsh', '==', latest_report.adsh)])

        return latest_filing.iloc[0].to_dict()

    def get_all_company_reports(self, forms: Optional[List[str]] = None) -> List[IndexReport]:
        """
        gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned

        Args:
            forms (List[str], optional, None): list of the forms to be returned,
             like ['10-Q', '10-K']

        Returns:
            List[IndexReport]: the list of matching reports as a list of IndexReport instances
        """
        return self.dbaccessor.read_index_reports_for_ciks([self.cik], forms)

    def get_all_company_reports_df(self, forms: Optional[List[str]] = None) -> pd.DataFrame:
        """
        gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned

        Args:
            forms (List[str], optional, None): list of the forms to
             be returned, like ['10-Q', '10-K']

        Returns:
            pd.DataFrame: the list of matching reports as a panas Dataframe
        """
        return self.dbaccessor.read_index_reports_for_ciks_df([self.cik], forms)
