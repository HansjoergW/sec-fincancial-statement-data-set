from secfsdstools.b_setup.setupdb import DbCreator


def test_db_creation(tmp_path):
    db_dir = str(tmp_path)

    creator = DbCreator(db_dir=db_dir)
    creator.create_db()

    # check if expected tables are present
    assert len(creator.execute_fetchall("SELECT * FROM index_parquet_processing_state")) == 0
    assert len(creator.execute_fetchall("SELECT * FROM index_parquet_reports")) == 0

    # check if column has_segments exists
    sql = "SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('index_parquet_processing_state') WHERE name = 'hasSegments'"
    result = creator.execute_fetchall(sql=sql)
    assert result[0][0] == 1


    