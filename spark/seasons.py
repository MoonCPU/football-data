from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql import Row

import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop' 
os.environ['PATH'] += ';C:\\hadoop\\bin' 


spark = SparkSession.builder \
    .appName("Football Matches Processing") \
    .config("spark.jars", "file:///C:/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

df_raw = spark.read.option("multiline", "true").json("../matches.json")

df_matches = df_raw.select(explode("matches").alias("match"))

first_row = df_matches.select("match.season").first()
season = first_row["season"]

season_id = season['id']
season_startDate = int(season['startDate'][:4])
season_endDate = int(season['endDate'][:4])
season_winner = season['winner'] if season['winner'] is not None else "N/A"

seasons_df = spark.createDataFrame([
    Row(season_id=season_id, 
        season_start_date=season_startDate, 
        season_end_date=season_endDate, 
        season_winner=season_winner)
])

jdbc_url = "jdbc:postgresql://localhost:5432/football_data" 
db_properties = {
    "user": "postgres",    
    "password": "soo020674",
    "driver": "org.postgresql.Driver"
}
table_name = "seasons"

try:
    seasons_df.write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .jdbc(url=jdbc_url,
                              table=table_name,
                              properties=db_properties)
    print(f"Data successfully written to table '{table_name}' in PostgreSQL.")
except Exception as e:
    print(f"Error writing data to PostgreSQL: {e}")

spark.stop()

# competition_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("../output/season")