from secfsdstools.b_setup.setupdb import DbCreator


def test_db_creation(tmp_path):
    db_dir = str(tmp_path)

    creator = DbCreator(db_dir=db_dir)
    creator.create_db()

    # check if expected tables are present
    assert len(creator.execute_fetchall("SELECT * FROM index_parquet_processing_state")) == 0
    assert len(creator.execute_fetchall("SELECT * FROM index_parquet_reports")) == 0
    