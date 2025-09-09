from connections.spark import spark
from config.config import config
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

kafka_topic = config.kafka_output_topic

event_schema = StructType([
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("avg_height", StringType(), True),
    StructField("avg_weight", StringType(), True),
    StructField("athlete_count", StringType(), True),
    StructField("time_of_calc", StringType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), event_schema).alias("data")) \
    .select("data.*")

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
