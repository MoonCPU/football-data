from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql import Row

import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop' 
os.environ['PATH'] += ';C:\\hadoop\\bin' 

spark = SparkSession.builder \
    .appName("Football Matches Processing") \
    .config("spark.jars", "file:///C:/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

df_raw = spark.read.option("multiline", "true").json("../competitions.json")

df_competitions = df_raw.select(explode("competitions").alias("competition"))

competitions_df_flat = df_competitions.select(
    col("competition.id").alias("competition_id"),
    col("competition.name").alias("competition_name"),
    col("competition.code").alias("competition_code"),
    col("competition.type").alias("competition_type")
)

# database url connection
jdbc_url = "jdbc:postgresql://localhost:5432/football_data" 
db_properties = {
    "user": "postgres",    
    "password": "soo020674",
    "driver": "org.postgresql.Driver"
}
table_name = "competitions"

try:
    competitions_df_flat.write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .jdbc(url=jdbc_url,
              table=table_name,
              properties=db_properties)
    print(f"Data successfully written to main table '{table_name}' in PostgreSQL.")
except Exception as e:
    print(f"Error writing data to PostgreSQL: {e}")

spark.stop()

# competition_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("../output/competition")