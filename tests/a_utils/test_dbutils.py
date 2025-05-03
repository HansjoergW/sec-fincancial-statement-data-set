from dataclasses import dataclass

import pytest
from secfsdstools.a_utils.dbutils import DB, DBStateAcessor

sql_create = """
    CREATE TABLE IF NOT EXISTS testtable1
        (
            col1,
            col2
        )
    """

sql_create_status = """
    CREATE TABLE IF NOT EXISTS status
    (
        keyName,
        value,
        PRIMARY KEY (keyName)
    );
    """


@pytest.fixture
def db(tmp_path) -> DB:
    return DB(db_dir=str(tmp_path))


@pytest.fixture
def dbstatus(tmp_path) -> DBStateAcessor:
    db = DBStateAcessor(db_dir=str(tmp_path))
    with db.get_connection() as conn:
        db.execute_single(sql_create_status, conn)
    return db


@dataclass
class DataRow:
    col1: str
    col2: str


def test_db_file_exists_no():
    assert DB(db_dir='blabli').db_file_exists() is False


def test_db_file_exists_yes(db: DB):
    with db.get_connection() as conn:
        db.execute_single(sql_create, conn)
    assert db.db_file_exists() is True


def test_table_exists(db: DB):
    assert db.table_exists('testtable1') is False

    with db.get_connection() as conn:
        db.execute_single(sql_create, conn)

    assert db.table_exists('testtable1') is True
    assert db.table_exists('bla') is False


def test_db(db: DB):
    # create simple table
    with db.get_connection() as conn:
        db.execute_single(sql_create, conn)

    # read the content
    result = db.execute_fetchall("SELECT * FROM testtable1")
    assert len(result) == 0

    # insert entries with execute_many
    sql = "INSERT INTO testtable1 ('col1', 'col2') VALUES (?, ?)"

    with db.get_connection() as conn:
        db.execute_many(sql, [('row1-1', 'row1-2'), ('row2-1', 'row2-2')], conn)

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


def test_insert_dataclass(db: DB):
    @dataclass
    class Row:
        col1: str
        col2: int

    # create simple table
    with db.get_connection() as conn:
        db.execute_single(sql_create, conn)

    data = Row(col1='col1', col2=123)
    insert_sql = db.create_insert_statement_for_dataclass(table_name='testtable1', data=data)
    assert insert_sql == "INSERT INTO testtable1 ('col1', 'col2') VALUES ('col1', 123)"


# --- Testing DBStateAccessor
def test_insert_and_overwrite(dbstatus: DBStateAcessor):
    key = 'key1'

    # read none existing key
    assert dbstatus.get_key(key=key) is None

    # insert new key
    dbstatus.set_key(key=key, value='value1')

    # read key
    assert dbstatus.get_key(key=key) == 'value1'

    # overwrite key
    dbstatus.set_key(key=key, value='value2')

    # read key
    assert dbstatus.get_key(key=key) == 'value2'
