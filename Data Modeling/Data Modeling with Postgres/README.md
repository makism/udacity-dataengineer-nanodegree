# SongPlays

- - -

In this project we are working with data related to songs and song plays; meaning logs that contain user information and metadata alongside with information about tracks.
We are asked first to go throught an iterative ETL process to start building the schemas in our database and the ETL process itself. At the end, we will incorporate the ETL processes into a script that goes over a number of files and applies our developed/proposed methods.

## Changelog

- 1.1
    [sql_queries]
        * Update insert conflict query for table `users`.
        * Change the type of field songplay_id` to `SERIAL` in table `songplays`.
        * Update relevant insert query for `songplays`.
    [etl.py]
        * Add missing function docstring.
        * Fetch the fields explicitly using indexing, i.e. df[["artist_id", "artist_name"]] 

- 1.0
    * Initial submittion.

## Project Organization

Below we'll describe the order and a short summary of each file provided within the project folder.

| order  | file             | description                                                    | execute                 |
|--------|------------------|----------------------------------------------------------------|-------------------------|
| 1.     | create_tables.py | Executes the queries defined in the help file `sql_queries.py`. First, it will drop any existing databases and tables, and then they will be created again                                                                                        | python create_tables.py |
| 2.     | etl.py           | The main entrypoint of our ETL application.                    | python etl.py           |

Other files that are part of our project include:

| file             | description                                                                                                    | comments                          |
|------------------|----------------------------------------------------------------------------------------------------------------|-----------------------------------|
| sql_queries.py   | Contains all the SQL statements for creating and dropping the tables; and all the necessary insert statement.  | The backbone of our DB is here.   |
| etl.ipynb        | The jupyter notebook in which we developed our ETL!                                                            | It's best viewed in JupyterLab ;) |
| test.ipynb       | A helper jupyter notebook for making SQL queries without hassle to our database; for testing purposes only!    | |


### ETL Processes

#### ETL

During the ETL process, we worked with a very small sample drawn out of the final data.
We loaded the JSON data using the appropriate methods from Pandas (pd.read_json) in a DataFrame (`df`).

The input data were formatted as follows:

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

so, we extracted the relevant fields:

```python

song_fields = ["song_id", "title", "artist_id", "year", "duration"]
song_data = df[song_fields].values.tolist()[0]

```

From the same Dataframe we extracted the artist's information with:

```python

artists_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
artist_data = df[artists_fields].values.tolist()[0]

```

In both cases, we stored the data in the tables `song` and `artists` accordingly

Later, we processed another batch of JSON files, that record the song plays in the format (truncated for readability reasons):

```json

{
   "artist":"Sydney Youngblood",
   "auth":"Logged In",
   "firstName":"Jacob",
   "gender":"M",
   "itemInSession":53,
   "lastName":"Klein",
   "length":238.07955,
   "level":"paid",
   "location":"Tampa-St. Petersburg-Clearwater, FL",
   "method":"PUT",
   "page":"NextSong",
   "registration":1540558108796.0,
   "sessionId":954,
   "song":"Ain't No Sunshine",
   "status":200,
   "ts":1543449657796,
   "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.78.2 (KHTML, like Gecko) Version\/7.0.6 Safari\/537.78.2\"",
   "userId":"73"
}

```

The first we were instructed to do was to filter the events, and keep only those whose action field was `NextSong`. Then we converted the timestamp (field `ts`) into an object using the tranformation:

```python

t = df['ts'] = pd.to_datetime(df['ts'], unit='ms')

```

Then, for each record in `t`, we extracted the timestamp, hour, day, week of year, month, year, and weekday; and subsequently inserted these rows into the table `time`.

From the same log file, we also extract information related to the user that listened the track and store them in the `users` table.

```python

user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

```

Finally, the most important aspect in our application, we combine both the songs table, artists table, and log file to fill in the table `songplays`. How we do that?

We iterate over the rows in the give log file, and:

1. First we query table `songs` and `artists` to fetch information (if any) for the specific `song title`, `artist id` and `track length` with:

```sql
SELECT song_id, artists.artist_id
FROM songs
    JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title = %s
    AND artists.name = %s
    AND songs.duration = %s
```

2. Then, we make a new insert statement with the following data to the `songplays` table:

```python

songplay_data = [
        pd.to_datetime(row['ts'], unit='ms'),
        row['userId'],
        row['level'],
        songid,
        artistid,
        row['sessionId'],
        row['location'],
        row['userAgent']
    ]

```

the underlying sql query resolves to:

```sql

INSERT INTO songplays ( ts, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO NOTHING;

```

#### Database schema

In general we have two types of schemas in our database, `facts`  that provide some kind of business-related information quickly, and `dimensions` that are the "building blocks" structures of all the information we store in our database.

##### Facts

Schema for table `songplays`; it records the songs (song_id, artist_id, etc) with some user metadata (location, user_agent) every time a user plays a song.

| column name | type       | extra options     | relation                     |
|-------------|------------|-------------------|------------------------------|
| songplay_id | SERIAL     | PRIMARY KEY       |                              |
| ts          | TIMESTAMP  | WITHOUT TIME ZONE |                              |
| user_id     | INT        | NOT NULL          | REFERENCES users(user_id)    |
| level       | TEXT       |                   |                              |
| song_id     | TEXT       |                   | REFERENCES songs(song_id)    |
| artist_id   | TEXT       |                   | REFERENCES artists(artist_id |
| session_id  | INT        |                   |                              |
| location    | TEXT       |                   |                              |
| user_agent  | TEXT       |                   |                              |


##### Dimensions

Schema for table `users` general information for a user such as, first and last name, gender (F/M) and level (free/paid).

| column name | type       | extra options     | relation                     |
|-------------|------------|-------------------|------------------------------|
| user_id     | INT        | PRIMARY KEY       |                              |
| first_name  | TEXT       | NOT NULL          |                              |
| last_name   | TEXT       | NOT NULL          |                              |
| gender      | TEXT       |                   |                              |
| level       | TEXT       |                   |                              |

Schema for table `song`; holds a song and information such as duration, year and an artist_id which can be resolved from table `artists`.

| column name | type       | extra options     | relation                     |
|-------------|------------|-------------------|------------------------------|
| user_id     | INT        | PRIMARY KEY       |                              |
| first_name  | TEXT       | NOT NULL          |                              |
| last_name   | TEXT       | NOT NULL          |                              |
| gender      | TEXT       |                   |                              |
| level       | TEXT       |                   |                              |

Schema for table `artists`; holds the artist's name and some geolocation information.

| column name | type       | extra options     | relation                     |
|-------------|------------|-------------------|------------------------------|
| artist_id   | INT        | PRIMARY KEY       |                              |
| name        | TEXT       | NOT NULL          |                              |
| location    | TEXT       |                   |                              |
| lattitude   | FLOAT      |                   |                              |
| longitude   | FLOAT      |                   |                              |

Schema for table `time`; holds the timestamp and date-time information for each song play.

| column name | type       | extra options     | relation                     |
|-------------|------------|-------------------|------------------------------|
| ts          | TIMESTAMP  | WITHOUT TIME ZONE PRIMARY KEY |                  |
| hour        | INT        |                   |                              |
| day         | INT        |                   |                              |
| week        | INT        |                   |                              |
| month       | INT        |                   |                              |
| year        | INT        |                   |                              |
| weekday     | INT        |                   |                              |


### ETL Pipeline

For our command-line pipeline we just filled-in the missing parts in the code.
The provided backbone of the application iterates automatically over the two directories,
one directory that stores the JSON files (in `data/song_data`) that contain information about the songs themselves. The other directory (`data/log_data`) contains the logs that records each event of users that listens to songs.

In the first directory, the method `process_song_file` is called for each json file. It takes two arguments, the first one, a valid connection to our database, and the second on the json complete filename.

For the second directory, the method `process_log_file` is called, with exactly the same parameters as the previous one.
