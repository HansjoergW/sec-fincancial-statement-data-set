from secfsdstools.update import update
from secfsdstools.a_config.configmgt import Configuration

if __name__ == '__main__':
    config = Configuration(db_dir="./../data/db",
                           parquet_dir="./../data/parquet",
                           download_dir="./../data/dld",
                           user_agent_email="hj@mycomp.com",
                           use_parquet=True
                           )

    update(config)