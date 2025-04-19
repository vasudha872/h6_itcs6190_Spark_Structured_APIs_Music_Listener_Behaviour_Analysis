# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1: User Favorite Genres
window = Window.partitionBy("user_id").orderBy(desc("count"))
logs.join(metadata, "song_id") \
    .groupBy("user_id", "genre") \
    .count() \
    .withColumn("rank", rank().over(window)) \
    .filter(col("rank") == 1) \
    .drop("rank") \
    .write.csv("output/user_favorite_genres", mode="overwrite")

# Task 2: Average Listen Time
logs.groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_duration_sec")) \
    .write.csv("output/avg_listen_time_per_song", mode="overwrite")

# Task 3: Top 10 Songs This Week (March 24-28, 2025)
logs.filter((col("timestamp") >= "2025-03-24") 
          & (col("timestamp") < "2025-03-29")) \
    .groupBy("song_id") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10) \
    .write.csv("output/top_songs_this_week", mode="overwrite")

# Task 4: Happy Song Recommendations
# Task 4: Happy Song Recommendations (Fixed)
sad_users = logs.join(metadata, "song_id") \
    .groupBy("user_id", "mood") \
    .count() \
    .groupBy("user_id") \
    .agg(max("mood").alias("primary_mood")) \
    .filter(col("primary_mood") == "Sad") \
    .select(col("user_id").alias("sad_user_id"))

# Get songs played by sad users
played_by_sad = logs.join(sad_users, logs.user_id == sad_users.sad_user_id) \
    .select("sad_user_id", "song_id")

# Get happy songs
happy_songs = metadata.filter(col("mood") == "Happy") \
    .select("song_id", "title")

# Generate recommendations using anti join and window function
recommendations = sad_users.crossJoin(happy_songs) \
    .join(played_by_sad, ["sad_user_id", "song_id"], "left_anti") \
    .withColumn("rn", row_number().over(
        Window.partitionBy("sad_user_id").orderBy(rand())
    )) \
    .filter(col("rn") <= 3) \
    .groupBy("sad_user_id") \
    .agg(collect_list("title").alias("recommended_songs"))

recommendations.write.json("output/happy_recommendations", mode="overwrite")

# Join to find happy songs not played by sad users
recommendations = happy_songs.alias("happy").join(played_songs.alias("played"), 
    (col("happy.song_id") == col("played.song_id")), 
    "left_anti") \
    .crossJoin(sad_users) \
    .groupBy("sad_user_id") \
    .agg(collect_list("happy.title").alias("recommended_songs")) \
    .limit(3)

recommendations.write.json("output/happy_recommendations", mode="overwrite")

# Task 5: Genre Loyalty Scores
user_genre_counts = logs.join(metadata, "song_id") \
    .groupBy("user_id", "genre") \
    .count()

total_plays = user_genre_counts.groupBy("user_id") \
    .agg(sum("count").alias("total"))

loyalty_scores = user_genre_counts.join(total_plays, "user_id") \
    .withColumn("loyalty_score", col("count")/col("total")) \
    .filter(col("loyalty_score") >= 0.8) \
    .write.csv("output/genre_loyalty_scores", mode="overwrite")

# Task 6: Identify users who listen between 12 AM and 5 AM
# Filter for night hours and count listens per user
(logs.filter(hour("timestamp").between(0, 5))
    .groupBy("user_id")
    .count()
    # Apply minimum play count threshold
    .filter(col("count") > 5)  
    .write.csv("output/night_owl_users", mode="overwrite"))

spark.stop()
