from connections import db
from connections import kafka
from config.config import config
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType
from streaming.data_processor import clean_bio, aggregate

def start_consumer():
    # Етап 1: читання статичних біологічних даних із MySQL - athlete_bio
    bio_df = db.read("athlete_bio")

    #Етап 2: фільтрація неповних/некоректних записів (height, weight)
    # - приводимо height/weight до double і відфільтровуємо ті, де cast дає null
    bio_clean = clean_bio(bio_df)

    print("Bio cleaned schema:")
    bio_clean.printSchema()
    bio_clean.show()

    event_schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", StringType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", StringType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True)
    ])

    # Етап 3: читання зі стрімінгового Kafka-topicu
    kafka_raw = kafka.read(config.kafka_input_topic)
    events = kafka_raw.selectExpr("CAST(value AS STRING) as json_string")
    events_parsed = events.select(from_json(col("json_string"), event_schema).alias("data")).select("data.*")
    events_parsed = events_parsed.withColumn("athlete_id", col("athlete_id").cast("string"))
    
    # Етап 4: Join стріму з static DataFrame (bio_clean)
    # - використаємо broadcast join (static DF невеликий), тож Spark автоматично оптимізує
    joined = events_parsed.join(bio_clean, on="athlete_id", how="inner")

    # Етап 5: Трансформації та агрегації
    # - обчислити середній height та weight по групах:
    #   (sport, medal (може бути null), sex, country_noc)
    # - додати current_timestamp() як time_of_calc
    aggregated = aggregate(joined)

    # Етап 6: Запис результатів кожного мікробатчу (forEachBatch)
    # 6.a) у вихідний Kafka-топік
    # 6.b) у MySQL таблицю
    def foreach_batch_function(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            print(f"Batch {batch_id}: empty, skipping.")
            return
        
        print(f"Processing batch id: {batch_id}, rows: {batch_df.count()}")

        kafka_out = batch_df.select(to_json(struct([batch_df[x] for x in batch_df.columns])).alias("value"))

        kafka.write(kafka_out, config.kafka_output_topic)

        db.write(batch_df, config.db_output_table)

        print(f"Batch {batch_id}: written to Kafka topic '{config.kafka_output_topic}' and to MySQL table '{config.db_output_table}'.")


    query = aggregated.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/athlete_streaming_enrichment") \
        .start()

    query.awaitTermination()
