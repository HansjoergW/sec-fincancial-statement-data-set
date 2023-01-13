Module secfsdstools.a_utils.dbutils
===================================
Basic DB handling functionality

Classes
-------

`DB(db_dir='db/')`
:   Base class for DB handling. Provides some basic functionality.

    ### Ancestors (in MRO)

    * abc.ABC

    ### Descendants

    * secfsdstools.b_setup.setupdb.DbCreator
    * secfsdstools.d_index.indexdataaccess.DBIndexingAccessor

    ### Methods

    `append_df_to_table(self, table_name: str, dataframe: pandas.core.frame.DataFrame)`
    :   add the content of a df to the table. The name of the columns in df
        and table have to match
        :param table_name: name of the table to append the data
        :param dataframe:  the df with the data

    `create_insert_statement_for_dataclass(self, table_name: str, data)`
    :   creates the insert sql statement based on the fields of a dataclass
        
        :param table_name: name of the table to insert into
        :param data: object of the dataclass
        :return: 'insert into' statement

    `execute_fetchall(self, sql: str) ‑> List[Tuple[]]`
    :   returns all results of the sql
        :param sql: sql statement
        :return: list with tuples

    `execute_fetchall_typed(self, sql: str, T) ‑> List[~T]`
    :   fetches all data of the sql statement and directly wraps it
        into the provided type.
        Note all selected columns in the sql have to exist with the same
         name in the dataclass of type T.
        
        :param sql: sql string
        :param T: type class
        :return: list of instances of the type

    `execute_many(self, sql: str, params: List[Tuple[]])`
    :   executes a parameterized statement for every tuple in the params list
        :param sql: parameterized statement
        :param params: list with tuples containing the parameters

    `execute_read_as_df(self, sql: str) ‑> pandas.core.frame.DataFrame`
    :   directly read the content into a pandas dataframe
        :param sql: Select String
        :return: pd.DataFrame

    `execute_single(self, sql: str)`
    :   executes a single sql statement without any parameters.
        :param sql: sql string, not paramterized

    `get_connection(self) ‑> sqlite3.Connection`
    :   creates a connection to the db.
        :return: sqlite3 connection instance