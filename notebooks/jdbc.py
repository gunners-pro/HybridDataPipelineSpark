import os
from pyspark.sql import SparkSession, DataFrame

def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    jdbc_url = (
        f"jdbc:sqlserver://{os.getenv('AZURE_SQL_HOST')}:1433;"
        f"database={os.getenv('AZURE_SQL_DB')};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
    )

    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", os.getenv("AZURE_SQL_USER")) \
        .option("password", os.getenv("AZURE_SQL_PASSWORD")) \
        .load()