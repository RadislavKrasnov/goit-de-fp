from config.config import config
from connections.spark import spark


def write(kafka_df, topic):
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.kafka_bootstrap) \
        .option("topic", topic) \
        .save()
    

def read(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.kafka_bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
