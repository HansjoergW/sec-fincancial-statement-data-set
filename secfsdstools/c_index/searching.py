"""
company search logic.
"""
from typing import Optional

import pandas as pd

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import DBIndexingAccessorBase, \
    create_index_accessor


class IndexSearch:
    """Provides search methods on the index_report table."""

    def __init__(self, dbaccessor: DBIndexingAccessorBase):
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

        accessor: DBIndexingAccessorBase = create_index_accessor(
            db_dir=configuration.db_dir)
        return IndexSearch(accessor)

    def find_company_by_name(self, name_part: str) -> pd.DataFrame:
        """
        Searches entries by the given name part. Upper/lower case is ignored.

        Args:
            name_part:

        Returns:
            pd.DataFrame: with columns 'cik', 'name'
        """

        return self.dbaccessor.find_company_by_name(name_part=name_part)
