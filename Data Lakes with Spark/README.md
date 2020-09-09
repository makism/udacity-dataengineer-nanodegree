```
       _____                  __   _ ____     
      / ___/____  ____ ______/ /__(_) __/_  __
      \__ \/ __ \/ __ `/ ___/ //_/ / /_/ / / /
     ___/ / /_/ / /_/ / /  / ,< / / __/ /_/ /
    /____/ .___/\__,_/_/  /_/|_/_/_/  \__, /  
        /_/                          /____/   
             a totally real startup company!
             
                       
                       v3.0 - AWS Data Lake
                       
```

## Intro

The purpose of our project is to load data from the given S3 buckets into Spark, and save the results back into a S3 bucket.


```
                         
+---------+       +----------------------------+      +---------+
|         |       |                            |      |         |
|         +------>+        Apache Spark        +----->+         |
|  S3     |       |                            |      |   S3    |
|         |       |                            |      |         |
|         |       +----------------------------+      |         |
+---------+                                           +---------+

```


The project builds upon previous projects (especially the one in Postgresql), so we are already a bit familiar with
the data, and their interpretation. As a refresher, we are working with data related to songs (see Example 1) and song plays;
meaning logs (see Example 2) that contain user information and metadata alongside with information about tracks.

> Exampe 1 - Snippet from the songs database; in JSON format.

```json
{
   "num_songs":1,
   "artist_id":"ARD0S291187B9B7BF5",
   "artist_latitude":null,
   "artist_longitude":null,
   "artist_location":"Ohio",
   "artist_name":"Rated R",
   "song_id":"SOMJBYD12A6D4F8557",
   "title":"Keepin It Real (Skit)",
   "duration":114.78159,
   "year":0
}
```


> Example 2 - Snippet from the event log; in JSON format.

```json
{
   "artist":null,
   "auth":"Logged In",
   "firstName":"Walter",
   "gender":"M",
   "itemInSession":0,
   "lastName":"Frye",
   "length":null,
   "level":"free",
   "location":"San Francisco-Oakland-Hayward, CA",
   "method":"GET",
   "page":"Home",
   "registration":1540919166796.0,
   "sessionId":38,
   "song":null,
   "status":200,
   "ts":1541105830796,
   "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
   "userId":"39"
}
```

## Main Project

| Name       | Description                                                                                         |
|------------|-----------------------------------------------------------------------------------------------------|
| etl.py     | The main entrypoint of our ETL data lake application                                                |
| config.py  | A few settings that control our application, such as `APP_DEV`, which makes the application locally |
| dl.cfg     | Configuration settings for our data late setup in AWS                                               |


> Please add your correct credentials in `dl.cfg` and edit `etl.py` with the correct output S3 Bucket.


Regarding, our main project; one have just to run the script `etl.py` with:

```bash

python etl.py

```

## Tables

The following table depicts the type of each table and which method creates it. 

| Function / ETL    | Tables      | Type      |
|-------------------|-------------|-----------|
| process_song_data | `songs`     | Dimension |
| process_song_data | `artist`    | Dimension |
| process_log_data  | `users`     | Dimension |
| process_log_data  | `time`      | Dimension |
| process_log_data  | `songplays` | Fact      |


### Fact Tables

> **songplays** - records in event data associated with song plays i.e. records with page NextSong

| Field       | Type      | Nullable |
|-------------|-----------|----------|
| songplay_id | Long      | False    |
| start_time  | Timestamp | True     |
| user_id     | String    | True     |
| level       | Double    | True     |
| song_id     | String    | True     |
| artist_id   | String    | True     |
| session_id  | Long      | True     |
| location    | String    | True     |
| user_agent  | String    | True     |

### Dimension Tables

> **songs** - songs in music database

| Field     | Type    | Nullable |
|-----------|---------|----------|
| song_id   | String  | True     |
| title     | String  | True     |
| artist_id | String  | True     |
| year      | Integer | True     |
| duration  | Double  | True     |

> **artists** - artists in music database

| Field     | Type    | Nullable |
|-----------|---------|----------|
| artist_id | String  | True     |
| name      | String  | True     |
| location  | String  | True     |
| lattitude | Double  | True     |
| longitude | Double  | True     |

> **users** - users in the app

| Field      | Type    | Nullable |
|------------|---------|----------|
| user_id    | Integer | True     |
| first_name | String  | True     |
| last_name  | String  | True     |
| gender     | String  | True     |
| level      | Double  | True     |

> **time** - timestamps of records in songplays broken down into specific units

| Field       | Type      | Nullable |
|-------------|-----------|----------|
| start_time  | Timestamp | True     |
| hour        | Integer   | True     | 
| day         | Integer   | True     |
| week        | Integer   | True     |
| month       | Integer   | True     |
| year        | Integer   | True     |
| weekday     | Integer   | True     |


Notes
-----

The uppercase types refer to Spark's types, such as `StringType`, `DoubleType`, etc.

## Development Notes

During the development cycle, we deployed the application locally (set `APP_DEV=True` in `config.py`), in our own Spark (running in local mode).
The files were read and written locally.
