from secfsdstools.b_setup.setupdb import DbCreator


def test_db_cration(tmp_path):
    db_dir = str(tmp_path)

    creator = DbCreator(db_dir=db_dir)
    creator.create_db()

    # check if expected tables are present
    assert len(creator.execute_fetchall("SELECT * FROM index_file_processing_state")) == 0
    assert len(creator.execute_fetchall("SELECT * FROM index_reports")) == 0
