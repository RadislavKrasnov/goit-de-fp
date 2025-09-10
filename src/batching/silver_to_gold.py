import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, expr


current_directory = os.path.dirname(os.path.abspath(__file__))


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("silver_to_gold") \
        .getOrCreate()
    
    os.makedirs('gold', exist_ok=True)

    bio_path = os.path.join(current_directory, 'silver/athlete_bio')
    events_path = os.path.join(current_directory, 'silver/athlete_event_results')

    if not os.path.exists(bio_path) or not os.path.exists(events_path):
        raise FileNotFoundError("Silver paths not found.")

    bio = spark.read.parquet(bio_path)
    events = spark.read.parquet(events_path)

    if 'height' in bio.columns:
        bio = bio.withColumn('height', expr("try_cast(height as double)"))

    if 'weight' in bio.columns:
        bio = bio.withColumn('weight', expr("try_cast(weight as double)"))

    bio = bio.withColumn('athlete_id', col('athlete_id').cast('string'))
    events = events.withColumn('athlete_id', col('athlete_id').cast('string'))

    bio = bio.drop("country_noc")
    
    joined = events.join(bio, on='athlete_id', how='inner')
    joined.show(10, truncate=False)

    grouped = joined.groupBy('sport', 'medal', 'sex', 'country_noc').agg(
        avg(col('weight')).alias('avg_weight'),
        avg(col('height')).alias('avg_height')
    ).withColumn('calculation_time', current_timestamp())
    grouped.show(50, truncate=False)

    out_path = os.path.join(current_directory, 'gold/avg_stats')
    grouped.write.mode('overwrite').parquet(out_path)
    print(f"Written gold data to {out_path}")

    spark.stop()
