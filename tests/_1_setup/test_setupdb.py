from secfsdstools._1_setup.setupdb import DbCreator


def test_db_cration(tmp_path):
    db_dir = str(tmp_path)

    creator = DbCreator(db_dir=db_dir)
    creator.create_db()

    # check if expected tables are present
    assert len(creator.execute_fetchall("SELECT * FROM zip_file_processing")) == 0
    assert len(creator.execute_fetchall("SELECT * FROM report_index")) == 0

