from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.functions import avg


def clean_bio(bio_df):
    return bio_df \
    .withColumn("height_double", expr("try_cast(height as double)")) \
    .withColumn("weight_double", expr("try_cast(weight as double)")) \
    .filter(col("height_double").isNotNull() & col("weight_double").isNotNull()) \
    .drop("height").drop("weight") \
    .drop("country_noc") \
    .withColumnRenamed("height_double", "height") \
    .withColumnRenamed("weight_double", "weight")


def aggregate(df):
    return df.groupBy(
        "sport", "medal", "sex", "country_noc"
    ).agg(
        avg(col("height")).alias("avg_height"),
        avg(col("weight")).alias("avg_weight"),
        expr("count(athlete_id) as athlete_count")
    ).withColumn("time_of_calc", current_timestamp())