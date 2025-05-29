from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, coalesce, lit
from dotenv import load_dotenv

import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop' 
os.environ['PATH'] += ';C:\\hadoop\\bin' 

load_dotenv()
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

spark = SparkSession.builder \
    .appName("Football Matches Processing") \
    .config("spark.jars", "file:///C:/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

df_raw = spark.read.option("multiline", "true").json("../all_matches_data.json")

df_matches = df_raw.select(explode("matches").alias("match"))

seasons_df_flat = df_matches.select(
    col("match.season.id").alias("season_id"),
    col("match.season.startDate").alias("startDate"),
    col("match.season.endDate").alias("endDate"),
    col("match.competition.id").alias("competition_id"),
    coalesce(col("match.season.winner"), lit("N/A")).alias("season_winner_name") 
)

seasons_distinct_df = seasons_df_flat.distinct()

seasons_final_df = seasons_distinct_df.select(
    col("season_id"),
    col("startDate").substr(1, 4).cast("bigint").alias("season_start_date"), 
    col("endDate").substr(1, 4).cast("int").alias("season_end_date"),  
    col("competition_id"),
    col("season_winner_name").alias("season_winner")
)

jdbc_url = "jdbc:postgresql://localhost:5432/football_data" 
db_properties = {
    "user": "postgres",    
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}
table_name = "staging.seasons"

try:
    seasons_final_df.write \
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