import requests
from pyspark.sql import SparkSession
import os


TABLES = ["athlete_bio", "athlete_event_results"]
current_directory = os.path.dirname(os.path.abspath(__file__))

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        file_path = os.path.join(current_directory, local_file_path)
        with open(file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("landing_to_bronze") \
        .getOrCreate()
    os.makedirs('bronze', exist_ok=True)
    
    for table in TABLES:
        csv_local = os.path.join(current_directory, f"{table}.csv")

        if not os.path.exists(csv_local):
            download_data(table)

        df = spark.read.option("header", True).option("inferSchema", True).csv(csv_local)
        df.show(10, truncate=False)
        out_path = os.path.join(current_directory, f"bronze/{table}")
        df.write.mode("overwrite").parquet(out_path)
        print(f"Written bronze data to {out_path}")
    
    spark.stop()
