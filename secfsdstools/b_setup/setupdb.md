Module secfsdstools.b_setup.setupdb
===================================
Creation of the database.

Classes
-------

`DbCreator(db_dir: str)`
:   responsible to  create the databse.

    ### Ancestors (in MRO)

    * secfsdstools.a_utils.dbutils.DB
    * abc.ABC

    ### Methods

    `create_db(self)`
    :   reads the ddl files from the ddl directory and creates the tables