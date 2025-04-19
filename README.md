# Music Streaming Analysis Using Spark Structured APIs

## Overview
This project analyzes user listening behavior and music trends using Spark Structured APIs. The structured data from a fictional music streaming platform is processed to gain insights into genre preferences, song popularity, and listener engagement.

## Dataset Description
Two datasets are used for this analysis:

### listening_logs.csv
Contains user listening activity logs:
- *user_id* – Unique ID of the user
- *song_id* – Unique ID of the song
- *timestamp* – Date and time when the song was played (e.g., 2025-03-23 14:05:00)
- *duration_sec* – Duration in seconds for which the song was played

### songs_metadata.csv
Contains metadata about the songs:
- *song_id* – Unique ID of the song
- *title* – Title of the song
- *artist* – Name of the artist
- *genre* – Genre of the song (e.g., Pop, Rock, Jazz)
- *mood* – Mood category of the song (e.g., Happy, Sad, Energetic, Chill)

## Tasks and Outputs
The following tasks were performed, and results were stored in structured output files.

1. *Find each user’s favorite genre*
   - Identified the most played genre for each user.
   - Output saved in output/user_favorite_genres/

2. *Calculate the average listen time per song*
   - Computed the average duration (in seconds) each song was played.
   - Output saved in output/avg_listen_time_per_song/

3. *List the top 10 most played songs this week*
   - Determined the top 10 most played songs in the current week.
   - Output saved in output/top_songs_this_week/

4. *Recommend “Happy” songs to users who mostly listen to “Sad” songs*
   - Found users with a preference for “Sad” songs and recommended up to 3 “Happy” songs they haven't played yet.
   - Output saved in output/happy_recommendations/

5. *Compute the genre loyalty score for each user*
   - Calculated the proportion of plays in the user's most-listened genre.
   - Output users with a loyalty score above 0.8.
   - Output saved in output/genre_loyalty_scores/

6. *Identify users who listen to music between 12 AM and 5 AM*
   - Extracted users who frequently listen to music late at night.
   - Output saved in output/night_owl_users/

7. *Generate an enriched listening logs dataset*
   - Joined listening_logs.csv with songs_metadata.csv for enhanced analysis.
   - Output saved in output/enriched_logs/

## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

You can run the analysis tasks either locally or using Docker.

#### *a. Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions

1. *Issue: Incorrect timestamp format during filtering*
   - *Solution:* Converted timestamps to the proper format using to_timestamp() in Spark SQL.
