"""
Basic DB handling functionality
"""

import logging
import os
import sqlite3
from abc import ABC
from dataclasses import Field
from typing import List, TypeVar, Tuple, Optional

import pandas as pd

T = TypeVar("T")  # pylint: disable=W0621

LOGGER = logging.getLogger(__name__)


# noinspection SqlResolve
class DB(ABC):
    """
    Base class for DB handling. Provides some basic functionality.
    """

    def __init__(self, db_dir="db/"):
        self.db_dir = db_dir
        self.database = os.path.join(self.db_dir, 'secfsdstools.db')

    def db_file_exists(self) -> bool:
        """
        Checks if the configured db files is actually present.

        Returns:
            bool: returns True, if the dbfile was found
        """
        return os.path.exists(self.database)

    def table_exists(self, table_name: str) -> bool:
        """
        Checks whether a table exists.

        Returns:
            bool: True if the  table is present
        """
        if not self.db_file_exists():
            return False

        # using the sqlite_master table to check whether the table exists
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        return len(self.execute_fetchall(sql)) > 0

    def get_connection(self) -> sqlite3.Connection:
        """
        creates a connection to the db.

        Returns:
            sqlite3.Connection: sqlite3 connection instance
        """
        return sqlite3.connect(self.database)

    def execute_read_as_df(self, sql: str) -> pd.DataFrame:
        """
        directly read the content into a pandas dataframe
        Args:
             sql (str): Select String
        Returns:
            pd.DataFrame: pd.DataFrame
        """
        conn = self.get_connection()
        try:
            LOGGER.debug("execute %s", sql)
            return pd.read_sql_query(sql, conn)
        finally:
            conn.close()

    def execute_fetchall(self, sql: str) -> List[Tuple]:
        """
        returns all results of the sql
        Args:
             sql (str): sql statement
        Returns:
            List[Tuple]: list with tuples
        """
        conn = self.get_connection()
        try:
            LOGGER.debug("execute %s", sql)
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    def execute_fetchall_typed(self, sql: str, T) -> List[T]:  # pylint: disable=W0621,C0103
        """fetches all data of the sql statement and directly wraps it
        into the provided type.
        Note all selected columns in the sql have to exist with the same
         name in the dataclass of type T.

        Args:
             sql (str): sql string
             T: type class
        Returns:
             List[T]: list of instances of the type
        """
        conn = self.get_connection()
        try:
            LOGGER.debug("execute %s", sql)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            return [T(**dict(x)) for x in results]
        finally:
            conn.close()

    def execute_single(self, sql: str, conn: sqlite3.Connection):
        """
        executes a single sql statement without any parameters.
        Args:
             sql (str): sql string, not paramterized
             conn (sqlite3.Connection): connection to use
        """
        LOGGER.debug("execute %s", sql)
        conn.execute(sql)

    def execute_many(self, sql: str, params: List[Tuple], conn: sqlite3.Connection):
        """
        executes a parameterized statement for every tuple in the params list
        Args:
             sql (str): parameterized statement
             params (List[Tuple]): list with tuples containing the parameters
             conn (sqlite3.Connection): connection to use
        """
        LOGGER.debug("execute %s", sql)
        conn.executemany(sql, params)

    def append_df_to_table(self, table_name: str, dataframe: pd.DataFrame,
                           conn: sqlite3.Connection):
        """
        add the content of a df to the table. The name of the columns in df
        and table have to match

        Args:
             table_name (str): name of the table to append the data
             dataframe (pd.DataFrame):  the df with the data
             conn (sqlite3.Connection): connection to use
        """
        dataframe.to_sql(table_name, conn, if_exists="append", index=False)

    def create_insert_statement_for_dataclass(self, table_name: str, data) -> str:
        """
        creates the insert sql statement based on the fields of a dataclass

        Args:
             table_name (str): name of the table to insert into
             data: object of the dataclass
        Returns:
            str: 'insert into' statement
        """
        # todo: None handling
        fields: List[Field]
        if isinstance(data.__dataclass_fields__, dict):
            # __dataclass_fields__ is a dict, so you can use the
            # .values() method to get the Field objects
            fields = data.__dataclass_fields__.values()
        else:  # from python 3.10
            # __dataclass_fields__ is a tuple, so you can just use it directly
            fields = data.__dataclass_fields__

        column_list = [f"'{field.name}'" for field in fields]
        value_list = []
        for field in fields:
            quotes = ""
            if field.type == str:
                quotes = "'"
            value_list.append(quotes + str(getattr(data, field.name)) + quotes)

        column_str = ', '.join(column_list)
        value_str = ', '.join(value_list)
        return f"INSERT INTO {table_name} ({column_str}) VALUES ({value_str})"


class DBStateAcessor(DB):
    """
    Helper class to write and read values into the status table
    """
    STATUS_TABLE_NAME = 'status'
    KEY_COL_NAME = 'keyName'
    VALUE_COL_NAME = 'value'

    def set_key(self, key: str, value: str):
        """
        Sets the provided key to the provided value.
        Args:
            key: key as string
            value: value as string
        """

        sql = f"""INSERT INTO {DBStateAcessor.STATUS_TABLE_NAME}
                                      ({DBStateAcessor.KEY_COL_NAME}, {DBStateAcessor.VALUE_COL_NAME})
                         VALUES ('{key}', '{value}') """

        # python 3.7 uses sqlite 3.21, which does not support the upsert functionality
        # with ON CONFLICT DO UPDATE SET
        # so we first have to check if the key exists and use update instead of insert
        if self.get_key(key):
            # update
            sql = f"""UPDATE {DBStateAcessor.STATUS_TABLE_NAME} 
                        SET {DBStateAcessor.VALUE_COL_NAME} = '{value}'
                        WHERE {DBStateAcessor.KEY_COL_NAME} = '{key}'"""

        with self.get_connection() as conn:
            self.execute_single(sql, conn)

    def get_key(self, key: str) -> Optional[str]:
        """
        Reads the value of key from the status table or returns None if the key is not present
        Args:
            key: key to read

        Returns:
            str: the stored value or None
        """
        sql = f"""SELECT {DBStateAcessor.VALUE_COL_NAME} 
                   FROM  {DBStateAcessor.STATUS_TABLE_NAME} 
                   WHERE {DBStateAcessor.KEY_COL_NAME} = '{key}'"""
        result = self.execute_fetchall(sql)

        return None if len(result) == 0 else result[0][0]

