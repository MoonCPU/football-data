from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, when, size, lit
from dotenv import load_dotenv

import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop' 
os.environ['PATH'] += ';C:\\hadoop\\bin' 

load_dotenv()
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

spark = SparkSession.builder.appName("Football Matches Processing").getOrCreate()

df_raw = spark.read.option("multiline", "true").json("../all_matches_data.json")

df_matches = df_raw.select(explode("matches").alias("match"))

df_flat = df_matches.select(
    col("match.id").alias("match_id"),
    col("match.area.id").alias("area_id"),
    col("match.competition.id").alias("competition_id"),
    col("match.season.id").alias("season_id"),     
    col("match.utcDate").alias("date"),
    col("match.status").alias("status"),
    col("match.matchday").alias("matchday"),
    col("match.stage").alias("stage"),
    col("match.homeTeam.name").alias("home_team"),
    col("match.awayTeam.name").alias("away_team"),
    when(col("match.score.winner") == "HOME_TEAM", col("match.homeTeam.name"))
        .when(col("match.score.winner") == "AWAY_TEAM", col("match.awayTeam.name"))
        .otherwise("Draw")
        .alias("winner"),
    col("match.score.fullTime.home").alias("home_score"),
    col("match.score.fullTime.away").alias("away_score"),
    col("match.score.halfTime.home").alias("home_ht_score"),
    col("match.score.halfTime.away").alias("away_ht_score"),
    when(size(col("match.referees")) > 0, col("match.referees").getItem(0).getItem("name"))
    .otherwise(lit("N/A")) 
    .alias("referee_name")
)

jdbc_url = "jdbc:postgresql://localhost:5432/football_data" 
db_properties = {
    "user": "postgres",    
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}
table_name = "staging.matches"

try:
    df_flat.write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .jdbc(url=jdbc_url,
                              table=table_name,
                              properties=db_properties)
    print(f"Data successfully written to table '{table_name}' in PostgreSQL.")
except Exception as e:
    print(f"Error writing data to PostgreSQL: {e}")

spark.stop()


# df_flat.show(5, truncate=False)

# df_flat.write.mode("overwrite").option("header", True).csv("../output/matches")