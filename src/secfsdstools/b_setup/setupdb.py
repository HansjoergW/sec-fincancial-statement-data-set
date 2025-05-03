"""
Creation of the database.
"""

import glob
import logging
import os
import sqlite3
from typing import Dict

from secfsdstools.a_utils.dbutils import DB

CURRENT_DIR, CURRENT_FILE = os.path.split(__file__)
DDL_PATH = os.path.join(CURRENT_DIR, "sql")

LOGGER = logging.getLogger(__name__)


class DbCreator(DB):
    """
    responsible to  create the database.
    """

    def __init__(self, db_dir: str):
        super().__init__(db_dir=db_dir)

    def add_column_if_not_exists(self, conn, table_name, column_name, data_type):
        """
        adds a column to an existing table, if the column does not exist

        Args:
            conn: connection
            table_name:  table_name
            column_name:  column_name to add
            data_type: data type of the column
        """
        try:
            cursor = conn.cursor()
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}")
            conn.commit()
            print(f"Column '{column_name}' added successfully.")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" in str(exc):
                print(f"Column '{column_name}' already exists.")
            else:
                print(f"An error occurred: {exc}")

    def create_db(self):
        """
        reads the ddl files from the ddl directory and creates the tables
        """

        sqlfiles = list(glob.glob(f"{DDL_PATH}/*.sql"))

        indexes_dict: Dict[int, str] = {}
        for sqlfile in sqlfiles:
            LOGGER.debug("extract version from sql file %s ... ", sqlfile)
            index = int(sqlfile[sqlfile.rfind(f'{os.path.sep}V') + 2:sqlfile.find('__')])
            LOGGER.debug(" ... extracted version: %d", index)
            indexes_dict[index] = sqlfile

        indexes = sorted(indexes_dict.keys())
        if not os.path.isdir(self.db_dir):
            LOGGER.info("creating folder for db: %s", self.db_dir)
            os.makedirs(self.db_dir)

        conn = self.get_connection()
        curr = conn.cursor()
        for index in indexes:
            sqlfile = indexes_dict[index]
            with open(sqlfile, 'r', encoding='utf8') as scriptfile:
                script = scriptfile.read()
                LOGGER.debug("execute creation script %s", sqlfile)
                curr.executescript(script)

            conn.commit()

        # adding columns that do not exist - so far none are needed

        conn.close()
