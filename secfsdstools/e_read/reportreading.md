Module secfsdstools.e_read.reportreading
========================================
reading and merging the data for a single report.

Functions
---------

    
`match_group_iter(match_iter)`
:   returns an iterator that returns the group() of the matching iterator
    :param match_iter:
    :return: group content iterator

Classes
-------

`BasicReportStats(num_entries: int, pre_entries: int, facts_per_date: Dict[int, int], list_of_statements: List[str], tags_per_statement: Dict[str, List[str]])`
:   Contains simple statistics of a report.

    ### Class variables

    `facts_per_date: Dict[int, int]`
    :

    `list_of_statements: List[str]`
    :

    `num_entries: int`
    :

    `pre_entries: int`
    :

    `tags_per_statement: Dict[str, List[str]]`
    :

`ReportReader(report: secfsdstools.d_index.indexdataaccess.IndexReport)`
:   reading the data for a single report. also provides several convenient methods
    to prepare and aggregate the raw data

    ### Static methods

    `get_report_by_adsh(adsh: str, configuration: Optional[secfsdstools.a_config.configmgt.Configuration] = None)`
    :   creates the ReportReader instance for a certain adsh.
        if no configuration is passed, it reads the config from the config file
        :param adsh: adsh
        :param configuration: Optional configuration object
        :return: instance of ReportReader

    `get_report_by_indexreport(index_report: secfsdstools.d_index.indexdataaccess.IndexReport)`
    :   crates the ReportReader instance based on the IndexReport instance
        :param index_report:
        :return:

    ### Methods

    `financial_statements_for_dates_and_tags(self, dates: Optional[List[int]] = None, tags: Optional[List[str]] = None) ‑> pandas.core.frame.DataFrame`
    :   creates the financial statements dataset by merging the pre and num
         sets together. It also filters out only the ddates that are
         inside the list.
        Note: the dates are int in the form YYYYMMDD
        :param dates: list with ddates to filter for
        :param tags: list with tags to consider
        :return: pd.DataFrame

    `financial_statements_for_period(self, tags: Optional[List[str]] = None) ‑> pandas.core.frame.DataFrame`
    :   returns the merged and pivoted table for the of the num-
         and predata for the current date only
        :param tags: List with tags to include or None
        :return:

    `financial_statements_for_period_and_previous_period(self, tags: Optional[List[str]] = None) ‑> pandas.core.frame.DataFrame`
    :   returns the merged and pivoted table for the of the num-
         and predata for the current and the date
         of the same period a year ago.
        :param tags: List with tags to include or None
        :return: pd.DataFrame

    `get_raw_num_data(self) ‑> pandas.core.frame.DataFrame`
    :   returns a copy of the raw dataframe for the num.txt file of this report
        :return: pd.DataFrame

    `get_raw_pre_data(self) ‑> pandas.core.frame.DataFrame`
    :   returns a copy of the raw dataframe for the pre.txt file of this report
        :return: pd.DataFrame

    `statistics(self) ‑> secfsdstools.e_read.reportreading.BasicReportStats`
    :   calculate a few simple statistics of a report.
        - number of entries in the num-file
        - number of entries in the pre-file
        - number of facts per ddate (in num file)
        - list of different statements in the pre file
        - list of tags per statement
        
        :return: BasicReportsStats instance