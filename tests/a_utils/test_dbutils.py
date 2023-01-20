from dataclasses import dataclass

import pytest

from secfsdstools.a_utils.dbutils import DB


@pytest.fixture
def db(tmp_path):
    db = DB(db_dir=str(tmp_path))
    return db


@dataclass
class DataRow:
    col1: str
    col2: str


sql_create = """
    CREATE TABLE IF NOT EXISTS testtable1
        (
            col1,
            col2
        )
    """


def test_db(db):
    # create simple table
    db.execute_single(sql_create)

    # read the content
    result = db.execute_fetchall("SELECT * FROM testtable1")
    assert len(result) == 0

    # insert entries with execute_many
    sql = "INSERT INTO testtable1 ('col1', 'col2') VALUES (?, ?)"

    db.execute_many(sql, [('row1-1', 'row1-2'), ('row2-1', 'row2-2')])

    # read the content without typing
    result = db.execute_fetchall("SELECT * FROM testtable1")
    assert len(result) == 2

    # read directly into dataframe
    result_df = db.execute_read_as_df("SELECT * FROM testtable1")
    assert result_df.shape == (2, 2)

    # read directly as type
    result_type = db.execute_fetchall_typed(sql="SELECT * FROM testtable1", T=DataRow)
    assert len(result_type) == 2
    assert result_type[0].col1 == 'row1-1'
    assert result_type[0].col2 == 'row1-2'

    print('success')

def test_insert_dataclass(db):
    @dataclass
    class Row:
        col1: str
        col2: int

    # create simple table
    db.execute_single(sql_create)
    
    data = Row(col1='col1', col2=123)
    insert_sql = db.create_insert_statement_for_dataclass(table_name='testtable1', data=data)
    assert insert_sql == "INSERT INTO testtable1 ('col1', 'col2') VALUES ('col1', 123)"