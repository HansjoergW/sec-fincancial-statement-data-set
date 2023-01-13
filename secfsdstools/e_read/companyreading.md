Module secfsdstools.e_read.companyreading
=========================================
Reads company information.

Classes
-------

`CompanyReader(cik: int, dbaccessor: secfsdstools.d_index.indexdataaccess.DBIndexingAccessor)`
:   reads information for a single company

    ### Static methods

    `get_company_reader(cik: int, configuration: Optional[secfsdstools.a_config.configmgt.Configuration] = None)`
    :   creates a company instance for the provided cik. If no  configuration object is passed,
        it reads the configuration from the config file.
        :param cik: cik
        :param configuration: Optional configuration object
        :return: instance of Company Reader

    ### Methods

    `get_all_company_reports(self, forms: Optional[List[str]] = None) ‑> List[secfsdstools.d_index.indexdataaccess.IndexReport]`
    :   gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned
        :param forms: list of the forms to be returend, like ['10-Q', '10-K']
        :return: List[IndexReport]

    `get_all_company_reports_df(self, forms: Optional[List[str]] = None) ‑> pandas.core.frame.DataFrame`
    :   gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned
        :param forms: list of the forms to be returend, like ['10-Q', '10-K']
        :return: pd.DataFrame

    `get_latest_company_filing(self) ‑> Dict[str, str]`
    :   returns the latest company information (the content in the sub.txt file)
        :return: Dict with str/str