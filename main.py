# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listeninglogs = spark.read.csv("/workspaces/h6_itcs6190_Spark_Structured_APIs_Music_Listener_Behaviour_Analysis/listening_logs.csv", header=True, inferSchema=True)
songsmetadata = spark.read.csv("/workspaces/h6_itcs6190_Spark_Structured_APIs_Music_Listener_Behaviour_Analysis/songs_metadata.csv", header=True, inferSchema=True)

# Task 1: User Favorite Genres
joined_df = listeninglogs.join(songsmetadata, on="song_id")
user_genre_counts = joined_df.groupBy("user_id", "genre").agg(count("*").alias("play_count"))
window_spec = Window.partitionBy("user_id").orderBy(col("play_count").desc())
user_fav_genre = user_genre_counts.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")
user_fav_genre.write.mode("overwrite").csv("output/userfavoritegenres", header=True)

# Task 2: Average Listen Time
avg_listen_time = listeninglogs.groupBy("song_id").agg(avg("duration_sec").alias("avg_durationsec"))
avg_listen_time.write.mode("overwrite").csv("output/avglistentimepersong", header=True)


# Task 3: Genre Loyalty Scores
total_plays = user_genre_counts.groupBy("user_id").agg(count("*").alias("total_plays"))
max_genre_plays = user_genre_counts.groupBy("user_id").agg(max("play_count").alias("max_genre_plays"))
loyalty_df = total_plays.join(max_genre_plays, on="user_id")
loyalty_df = loyalty_df.withColumn("loyalty_score", col("max_genre_plays") / col("total_plays"))
loyal_users = loyalty_df.filter(col("loyalty_score") > 0.8)
loyal_users.write.mode("overwrite").csv("output/genreloyaltyscores", header=True)

# Task 4: Identify users who listen between 12 AM and 5 AM
listeninglogs = listeninglogs.withColumn("hour", hour(col("timestamp")))
night_owl_users = listeninglogs.filter((col("hour") >= 0) & (col("hour") < 5)).select("user_id").distinct()
night_owl_users.write.mode("overwrite").csv("output/nightowlusers", header=True)

# Stop Spark session at the end
spark.stop()
