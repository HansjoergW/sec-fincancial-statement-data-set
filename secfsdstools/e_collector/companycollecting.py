"""
Collects all data for a company.
"""
from typing import Optional, List

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexReport, ParquetDBIndexingAccessor
from secfsdstools.e_collector.multireportcollecting import MultiReportCollector


class CompanyReportCollector:
    """
    Collects reports for a single company (cik) from different  zip files.
    For instance, it is a simple to read all 10-K reports.
    """

    @classmethod
    def get_company_collector(
            cls, cik: int,
            forms_filter: Optional[List[str]] = None,
            stmt_filter: Optional[List[str]] = None,
            tag_filter: Optional[List[str]] = None,
            configuration: Optional[Configuration] = None) -> MultiReportCollector:
        """
        creates a MultiReportCollector instance for the provided cik and forms (e.g. 10-K..)
        If no configuration object is passed,
        it reads the configuration from the config file.

        Args:
            cik (int): the central identification key which is assigned by the sec for every company
            forms_filter (List[str], optional, None):
                List of forms that should be read (10-K, 10-Q, ...)
            stmt_filter (List[str], optional, None):
                List of stmts that should be read (BS, IS, ...)
            tag_filter (List[str], optional, None:
                List of tags that should be read (Assets, Liabilities, ...)
            configuration (Configuration, optional, None): Optional configuration object

        Returns:
            MultiReportCollector: instance of MultiReportCollector
        """

        if configuration is None:
            configuration = ConfigurationManager.read_config_file()

        dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)

        # todo: if daily entries are also in index, it returns mutliple matches!
        #       probably fix directly in read_index_reports-> filter for two and check source
        #       prefer to use zip instead of daly?
        # todo: multiple ciks should be possible
        index_reports: List[IndexReport] = dbaccessor.read_index_reports_for_cik(cik, forms_filter)

        return MultiReportCollector.get_reports_by_indexreports(index_reports=index_reports,
                                                                stmt_filter=stmt_filter,
                                                                tag_filter=tag_filter
                                                                )
