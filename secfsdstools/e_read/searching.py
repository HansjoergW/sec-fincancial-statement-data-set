"""
company search logic.
"""
from typing import Optional

import pandas as pd

from secfsdstools.a_config.configmgt import Configuration, ConfigurationManager
from secfsdstools.d_index.indexdataaccess import DBIndexingAccessor


class IndexSearch:
    """Provides search methods on the index_report table."""

    def __init__(self, dbaccessor: DBIndexingAccessor):
        self.dbaccessor = dbaccessor

    @classmethod
    def get_index_search(cls, configuration: Optional[Configuration] = None):
        """
        Creates a IndexSearch instance.
        If no  configuration object is passed, it reads the configuration from
        the config file.
        Args:
            configuration (Configuration, optional, None): configuration object

        Returns:
            IndexSearch: instance of IndexSearch
        """
        if configuration is None:
            configuration = ConfigurationManager.read_config_file()
        return IndexSearch(DBIndexingAccessor(db_dir=configuration.db_dir))


    def find_company_by_name(self, name_part: str) -> pd.DataFrame:
        """
        Searches entries by the given name part. Upper/lower case is ignored.

        Args:
            name_part:

        Returns:
            pd.DataFrame: with columns 'cik', 'name'
        """

        return self.dbaccessor.find_company_by_name(name_part=name_part)
