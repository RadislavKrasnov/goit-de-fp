from connections import db
from connections import kafka
from config.config import config
from pyspark.sql.functions import to_json, struct
from connections.spark import spark


def start_producer():
    """
    Етап 3. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results. 
    """
    event_results_df = db.read('athlete_event_results')

    event_results_df.show(5, truncate=False)
    event_results_df.printSchema()

    kafka_df = event_results_df.select(to_json(struct([event_results_df[x] for x in event_results_df.columns])).alias("value"))

    kafka_df.show(5, truncate=False)


    kafka.write(kafka_df, config.kafka_input_topic)

    print("Finished producing athlete_event_results to Kafka.")
