from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from dotenv import load_dotenv

import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop' 
os.environ['PATH'] += ';C:\\hadoop\\bin' 

load_dotenv()
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

spark = SparkSession.builder \
    .appName("Football Areas Processing") \
    .config("spark.jars", "file:///C:/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

df_raw = spark.read.option("multiline", "true").json("../areas.json")

df_areas = df_raw.select(explode("areas").alias("area"))

areas_df_flat = df_areas.select(
    col("area.id").alias("area_id"),
    col("area.name").alias("area_name"),
    col("area.countryCode").alias("area_country_code")
)

jdbc_url = "jdbc:postgresql://localhost:5432/football_data" 
db_properties = {
    "user": "postgres",    
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}
table_name = "areas"

try:
    areas_df_flat.write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .jdbc(url=jdbc_url,
              table=table_name,
              properties=db_properties)
    print(f"Data successfully written to main table '{table_name}' in PostgreSQL.")
except Exception as e:
    print(f"Error writing data to PostgreSQL: {e}")

spark.stop()

# competition_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("../output/area")