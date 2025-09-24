#  Music Streaming Analysis Using Spark Structured APIs

##  Overview
This project analyzes **user listening behavior** and **music trends** using **Apache Spark Structured APIs**.  
The analysis is based on two datasets from a fictional music streaming platform:
- **listening_logs.csv** → User listening activity logs  
- **songs_metadata.csv** → Metadata about songs  

Using Spark DataFrames and SQL-style transformations, we extract insights about:
- User favorite genres  
- Average listen times  
- Genre loyalty scores  
- Night owl users  

---

##  Dataset Description

### listening_logs.csv
| Column       | Description                                | Example             |
|--------------|--------------------------------------------|---------------------|
| user_id      | Unique ID of the user                      | user_15             |
| song_id      | Unique ID of the song                      | song_23             |
| timestamp    | Date and time when the song was played     | 2025-03-23 14:05:00 |
| duration_sec | Duration listened (in seconds)             | 215                 |

### songs_metadata.csv
| Column   | Description              | Example        |
|----------|--------------------------|----------------|
| song_id  | Unique ID of the song    | song_23        |
| title    | Title of the song        | Title_song_23  |
| artist   | Artist name              | Artist_7       |
| genre    | Genre of the song        | Pop            |
| mood     | Mood category of the song| Energetic      |

---

##  Repository Structure
```bash
├── README.md
├── datagen.py / input_generator.py   # Script to generate datasets
├── main.py                           # Spark analysis code
├── listening_logs.csv                # Sample generated input
├── songs_metadata.csv                # Sample generated input
├── outputs/
│   ├── user_favorite_genres/
│   │   ├── part-*.csv
│   ├── avg_listen_time/
│   │   ├── part-*.csv
│   ├── genre_loyalty_scores/
│   │   ├── part-*.csv
│   ├── night_owl_users/
│   │   ├── part-*.csv
```

## Output Directory Structure
Results of each task are saved as CSV files in their respective folders within the `outputs/` directory:
```bash
├── output
│   ├── userfavoritegenres
│   │   ├── **.csv
│   ├── avglistentimepersong
│   │   ├── **.csv
│   ├── genreloyaltyscores
│   │   ├── **.csv
│   ├── nightowlusers
│   │   ├── **.csv
```

## Tasks and Outputs
### Task 1: User Favourite Genre  
** Task:** Join user listening logs with song metadata on `songid`, group by `userid` and `genre`, count the number of plays per genre, and determine the favorite genre per user as the genre with the maximum plays.  
** Output Content:** CSV with columns:  
- `user_id`  
- `genre` (user's favorite genre)  
- `play_count` (number of plays in that genre)

### Task 2: Average Listen Time per Song  
**Task:** Aggregate listening logs by `songid` to compute the average listen duration (`durationsec`) for each song.  
**Output Content:** CSV with columns:  
- `song_id`  
- `avg_durationsec` (average listening duration in seconds)

### Task 3: Genre Loyalty Score  
**Task:** Calculate for each user the total plays and plays in their favorite genre, compute loyalty score as the ratio, and filter users with loyalty scores greater than 0.8.  
**Output Content:** CSV with columns:  
- `user_id`  
- `total_plays`  
- `max_genre_plays`  
- `loyalty_score`

### Task 4: Night Owl Users  
**Task:** Identify users who listen between midnight and 5 AM by filtering listening log timestamps and selecting distinct `userid`s.  
**Output Content:** CSV with a single column:  
- `user_id`


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

####  *Running Locally*

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
### Example Error: AnalysisException
You may encounter the following error when joining your dataframes:
AnalysisException: [UNRESOLVED_USING_COLUMN_FOR_JOIN] USING column songid cannot be resolved on the left side of the join. The left-side columns: [duration_sec, song_id, timestamp, user_id].

#### Cause
This error occurs when the column names used for joining (`songid` in this case) do not match between the two dataframes. For example, your `listeninglogs` dataframe may have the column named `song_id`, while your join command expects `songid`.

#### Resolution
Ensure the column names are consistent across dataframes before joining. If there is a mismatch, rename the columns in your dataframe using the following code:
```bash
listeninglogs = listeninglogs.withColumnRenamed("song_id", "songid")\
                             .withColumnRenamed("user_id", "userid")\
                             .withColumnRenamed("duration_sec", "durationsec")
joined_df = listeninglogs.join(songsmetadata, on="songid")
 ```
Use the updated column names in all your analysis code. This resolves the error and allows the join to succeed.
