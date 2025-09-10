import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


TABLES = ["athlete_bio", "athlete_event_results"]
current_directory = os.path.dirname(os.path.abspath(__file__))


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("bronze_to_silver") \
        .getOrCreate()
    
    os.makedirs('silver', exist_ok=True)

    clean_text_udf = udf(clean_text, StringType())

    for table in TABLES:
        bronze_path = os.path.join(current_directory, f"bronze/{table}")

        if not os.path.exists(bronze_path):
            raise FileNotFoundError(f"Bronze path not found: {bronze_path}.")
        
        df = spark.read.parquet(bronze_path)

        for (c_name, c_type) in df.dtypes:
            if c_type == 'string':
                df = df.withColumn(c_name, clean_text_udf(col(c_name)))

        before = df.count()
        df = df.dropDuplicates()
        after = df.count()
        print(f"Rows before dedup: {before}, after dedup: {after}")

        df.show(10, truncate=False)

        out_path = os.path.join(current_directory, f"silver/{table}")
        df.write.mode("overwrite").parquet(out_path)
        print(f"Written silver data to {out_path}")

    spark.stop()
