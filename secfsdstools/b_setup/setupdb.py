"""
Creation of the database.
"""

import glob
import logging
import os
from typing import Dict

from secfsdstools.a_utils.dbutils import DB

CURRENT_DIR, CURRENT_FILE = os.path.split(__file__)
DDL_PATH = os.path.join(CURRENT_DIR, "sql")

LOGGER = logging.getLogger(__name__)


class DbCreator(DB):
    """
    responsible to  create the databse.
    """

    def __init__(self, db_dir: str):
        super().__init__(db_dir=db_dir)

    def create_db(self):
        """
        reads the ddl files from the ddl directory and creates the tables
        """

        sqlfiles = list(glob.glob(DDL_PATH + "/*.sql"))

        indexes_dict: Dict[int, str] = {}
        for sqlfile in sqlfiles:
            LOGGER.debug("extract version from sql file %s ... ", sqlfile)
            index = int(sqlfile[sqlfile.rfind(os.path.sep + 'V') + 2:sqlfile.find('__')])
            LOGGER.debug(" ... extracted version: %d", index)
            indexes_dict[index] = sqlfile

        indexes = list(indexes_dict.keys())
        indexes.sort()

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
        conn.close()
