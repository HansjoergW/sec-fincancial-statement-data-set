"""
Reads company information.
"""
import re
from typing import Dict, Optional, List

import pandas as pd

from secfsdstools.a_config.configmgt import Configuration, ConfigurationManager
from secfsdstools.a_utils.fileutils import read_content_from_file_in_zip
from secfsdstools.d_index.indexdataaccess import DBIndexingAccessor, IndexReport

SUB_TXT = "sub.txt"


class CompanyReader:
    """
    reads information for a single company
    """

    @classmethod
    def get_company_reader(cls, cik: int, configuration: Optional[Configuration] = None):
        """
        creates a company instance for the provided cik. If no  configuration object is passed,
        it reads the configuration from the config file.
        :param cik: cik
        :param configuration: Optional configuration object
        :return: instance of Company Reader
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
        :return: Dict with str/str
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
        :param forms: list of the forms to be returend, like ['10-Q', '10-K']
        :return: List[IndexReport]
        """
        return self.dbaccessor.read_index_reports_for_cik(self.cik, forms)

    def get_all_company_reports_df(self, forms: Optional[List[str]] = None) -> pd.DataFrame:
        """
        gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned
        :param forms: list of the forms to be returend, like ['10-Q', '10-K']
        :return: pd.DataFrame
        """
        return self.dbaccessor.read_index_reports_for_cik_df(self.cik, forms)
