from connections.spark import spark
from config.config import config

def write(batch_df, table_name: str):
    (batch_df
        .write
        .format("jdbc")
        .option("url", config.db_url)
        .option("driver", config.db_driver)
        .option("dbtable", table_name)
        .option("user", config.db_user)
        .option("password", config.db_password)
        .mode("append")
        .save()
    )

def read(table_name: str):
    return spark.read.format("jdbc").options(
        url=config.db_url,
        driver=config.db_driver,
        dbtable=table_name,
        user=config.db_user,
        password=config.db_password
    ).load()
