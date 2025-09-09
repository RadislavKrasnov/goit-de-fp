from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    def __init__(self):
        self.db_url: str = os.getenv("JDBC_URL")
        self.db_user: str = os.getenv("JDBC_USER")
        self.db_password: str = os.getenv("JDBC_PASSWORD")
        self.db_driver: str = os.getenv("JDBC_DRIVER")
        self.mysql_jar: str = os.getenv("MYSQL_JAR")
        self.kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP")
        self.kafka_input_topic: str = os.getenv("INPUT_TOPIC")
        self.kafka_output_topic: str = os.getenv("OUTPUT_TOPIC")
        self.db_output_table: str = os.getenv("OUTPUT_JDBC_TABLE")
        self.kafka_jar: str = os.getenv("KAFKA_JAR")
        self.kafka_client_jar: str = os.getenv("KAFKA_CLIENT_JAR")


config = Config()
