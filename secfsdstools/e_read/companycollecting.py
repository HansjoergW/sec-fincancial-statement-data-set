"""
Collects all data for a company.
"""
from typing import Optional, List

from secfsdstools.a_config.configmgt import Configuration, ConfigurationManager
from secfsdstools.d_index.indexdataaccess import DBIndexingAccessor, IndexReport
from secfsdstools.e_read.multireportreading import MultiReportReader


class CompanyReportCollector:
    """
    Collects reports for a single company (cik) from different  zip files.
    For instance, it is a simple to read all 10-K reports.
    """

    @classmethod
    def get_company_collector(cls, cik: int, forms: Optional[List[str]] = None,
                              configuration: Optional[Configuration] = None):
        """
        creates a MulitReportReader instance for the provided cik and forms (e.g. 10-K..)
        If no configuration object is passed,
        it reads the configuration from the config file.

        Args:
            cik (int): the central identification key which is assigned by the sec for every company
            forms: a list of forms which should be collected, like 10-K, or 10 Q
            configuration (Configuration, optional, None): Optional configuration object

        Returns:
            MultiReportReader: instance of Multi Collector
        """

        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = DBIndexingAccessor(db_dir=configuration.db_dir)
        index_reports: List[IndexReport] = dbaccessor.read_index_reports_for_cik(cik, forms)

        return MultiReportReader(index_reports=index_reports)
