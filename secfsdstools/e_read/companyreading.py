"""
Reads company information.
"""
import re
from typing import Dict, Optional, List

import pandas as pd

from secfsdstools.a_config.configmgt import Configuration, ConfigurationManager
from secfsdstools.a_utils.fileutils import read_content_from_file_in_zip
from secfsdstools.d_index.indexdataaccess import DBIndexingAccessor, IndexReport
from secfsdstools.e_read.basereportreading import SUB_TXT


class CompanyReader:
    """
    reads information for a single company
    """

    @classmethod
    def get_company_reader(cls, cik: int, configuration: Optional[Configuration] = None):
        """
        creates a company instance for the provided cik. If no  configuration object is passed,
        it reads the configuration from the config file.

        Args:
            cik (int): the central identification key which is assigned by the sec for every company
            configuration (Configuration, optional, None): Optional configuration object

        Returns:
            CompanyReader: instance of Company Reader
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()
        return CompanyReader(cik, DBIndexingAccessor(db_dir=configuration.db_dir))

    def __init__(self, cik: int, dbaccessor: DBIndexingAccessor):
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
        content = read_content_from_file_in_zip(latest_report.fullPath, SUB_TXT)

        newline_index = content.index('\n')
        colnames = content[:newline_index]

        adsh_pattern = re.compile(f'^{latest_report.adsh}.*$', re.MULTILINE)
        values = adsh_pattern.search(content).group()

        value_dict: Dict[str, str] = \
            {x[0]: x[1] for x in zip(colnames.split('\t'), values.split('\t'))}
        return value_dict

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
        return self.dbaccessor.read_index_reports_for_cik(self.cik, forms)

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
        return self.dbaccessor.read_index_reports_for_cik_df(self.cik, forms)
