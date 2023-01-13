Module secfsdstools.d_index.indexdataaccess
===========================================
Database logic to hanlde the indexing

Classes
-------

`DBIndexingAccessor(db_dir: str)`
:   Dataaccess class for index related tables

    ### Ancestors (in MRO)

    * secfsdstools.a_utils.dbutils.DB
    * abc.ABC

    ### Class variables

    `INDEX_PROCESSING_TABLE`
    :

    `INDEX_REPORTS_TABLE`
    :

    ### Methods

    `append_indexreport_df(self, dataframe: pandas.core.frame.DataFrame)`
    :   append the content of the df to the index report table
        :param dataframe: the dataframe to be appended

    `find_latest_company_report(self, cik: int) ‑> secfsdstools.d_index.indexdataaccess.IndexReport`
    :   returns the latest report of a company
        :param cik: the cik of the company
        :return:IndexReport of the latest report of the company

    `insert_indexfileprocessing(self, data: secfsdstools.d_index.indexdataaccess.IndexFileProcessingState)`
    :   inserts an entry into the index_file_processing_state table
        :param data: IndexFileProcessingState data object

    `insert_indexreport(self, data: secfsdstools.d_index.indexdataaccess.IndexReport)`
    :   inserts an entry into the index_report table
        :param data: IndexReport data object

    `read_all_indexfileprocessing(self) ‑> List[secfsdstools.d_index.indexdataaccess.IndexFileProcessingState]`
    :   reads all entries of the index_file_processing_state table
        :return: List with IndexFileProcessingState objects

    `read_all_indexfileprocessing_df(self) ‑> pandas.core.frame.DataFrame`
    :   reads all entries of the index_file_processing_state table as a pandas DataFrame
        :return: pandas DataFrame

    `read_all_indexreports(self) ‑> List[secfsdstools.d_index.indexdataaccess.IndexReport]`
    :   reads all entries of the index_reports table
        :return: List with IndexReport objects

    `read_all_indexreports_df(self) ‑> pandas.core.frame.DataFrame`
    :   reads all entries of the index_reports table as a pandas DataFrame
        :return: pandas DataFrame

    `read_index_report_for_adsh(self, adsh: str) ‑> secfsdstools.d_index.indexdataaccess.IndexReport`
    :   returns the IndexReport instance for the provided adsh
        :param adsh: adsh
        :return: the report for the provided adsh

    `read_index_reports_for_cik(self, cik: int, forms: Optional[List[str]] = None) ‑> List[secfsdstools.d_index.indexdataaccess.IndexReport]`
    :   gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned
        :param cik: cik of the company
        :param forms: list of the forms to be returend, like ['10-Q', '10-K']
        :return:

    `read_index_reports_for_cik_df(self, cik: int, forms: Optional[List[str]] = None) ‑> pandas.core.frame.DataFrame`
    :   gets all reports as IndexReport instances for a company identified by its cik.
        if forms is not set, all forms are returned, otherwise forms is a list of the
         forms that should be returned
        :param cik: cik of the company
        :param forms: list of the forms to be returend, like ['10-Q', '10-K']
        :return:

`IndexFileProcessingState(fileName: str, fullPath: str, status: str, entries: int, processTime: str)`
:   dataclass for index_file_processing_state table

    ### Class variables

    `entries: int`
    :

    `fileName: str`
    :

    `fullPath: str`
    :

    `processTime: str`
    :

    `status: str`
    :

`IndexReport(adsh: str, cik: int, name: str, form: str, filed: int, period: int, fullPath: str, originFile: str, originFileType: str, url: str)`
:   dataclass for index_reports table

    ### Class variables

    `adsh: str`
    :

    `cik: int`
    :

    `filed: int`
    :

    `form: str`
    :

    `fullPath: str`
    :

    `name: str`
    :

    `originFile: str`
    :

    `originFileType: str`
    :

    `period: int`
    :

    `url: str`
    :