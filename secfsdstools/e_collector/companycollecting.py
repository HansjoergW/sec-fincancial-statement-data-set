"""
Collects all data by the cik company.
"""
from typing import Optional, List

from secfsdstools.a_config.configmgt import ConfigurationManager
from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_index.indexdataaccess import IndexReport, ParquetDBIndexingAccessor
from secfsdstools.e_collector.multireportcollecting import MultiReportCollector


class CompanyReportCollector:
    """
    Collects reports for a companies defined by their cik number.
    Collects the data from different  zip files.
    For instance, it is a simple way to read all 10-K reports of serveral companies.
    """

    @classmethod
    def get_company_collector(
            cls, ciks: List[int],
            forms_filter: Optional[List[str]] = None,
            stmt_filter: Optional[List[str]] = None,
            tag_filter: Optional[List[str]] = None,
            configuration: Optional[Configuration] = None):
        """
        creates a MultiReportCollector instance for the provided ciks and forms (e.g. 10-K..)
        If no configuration object is passed,
        it reads the configuration from the configuration file.

        Args:
            ciks (List[int]): a list of central identification keys which is assigned
                             by the sec to every company
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
        #       probably fix directly in read_index_reports-> pathfilter for two and check source
        #       prefer to use zip instead of daly?
        index_reports: List[IndexReport] = dbaccessor.read_index_reports_for_ciks(ciks,
                                                                                  forms_filter)

        return MultiReportCollector.get_reports_by_indexreports(index_reports=index_reports,
                                                                stmt_filter=stmt_filter,
                                                                tag_filter=tag_filter
                                                                )
