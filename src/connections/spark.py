from pyspark.sql import SparkSession
from config.config import config

class SparkSingleton:
    _instance = None

    @classmethod
    def get_spark(cls) -> SparkSession:
        if cls._instance is None:
            cls._instance = SparkSession.builder \
                .appName("StreamingAthleteEnrichment") \
                .config("spark.jars.packages", f"{config.mysql_jar},{config.kafka_jar},{config.kafka_client_jar}") \
                .getOrCreate()
            cls._instance.sparkContext.setLogLevel("WARN")
        return cls._instance

spark = SparkSingleton.get_spark()
