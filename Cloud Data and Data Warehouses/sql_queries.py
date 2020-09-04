import configparser

#
# CONFIG
#
config = configparser.ConfigParser()
config.read('dwh.cfg')

#
# DROP TABLES
#
staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS stagingsongs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS songs;"
artist_table_drop         = "DROP TABLE IF EXISTS artists;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

#
# CREATE TABLES
#
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stagingevents (
        event_id      BIGINT IDENTITY(0,1)  NOT NULL,
        artist        VARCHAR               NULL,
        auth          VARCHAR(100)          NULL,
        firstName     VARCHAR               NULL,
        gender        VARCHAR(2)            NULL,
        itemInSession VARCHAR               NULL,
        lastName      VARCHAR               NULL,
        length        VARCHAR               NULL,
        level         VARCHAR(5)            NULL,
        location      VARCHAR(256)          NULL,
        method        VARCHAR(10)           NULL,
        page          VARCHAR(100)          NULL,
        registration  VARCHAR               NULL,
        sessionId     INTEGER               NOT NULL SORTKEY DISTKEY,
        song          VARCHAR               NULL,
        status        INTEGER               NULL,
        ts            BIGINT                NOT NULL,
        userAgent     VARCHAR               NULL,
        userId        INTEGER               NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stagingsongs (
        num_songs         INTEGER       NULL,
        artist_id         VARCHAR(256)  NOT NULL SORTKEY DISTKEY,
        artist_latitude   DECIMAL       NULL,
        artist_longitude  DECIMAL       NULL,
        artist_location   VARCHAR(256)  NULL,
        artist_name       VARCHAR(256)  NULL,
        song_id           VARCHAR(18)   NOT NULL,
        title             TEXT          NULL,
        duration          DECIMAL(10)   NULL,
        year              INTEGER       NULL DEFAULT(0)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id  INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
        start_time   TIMESTAMP               NOT NULL,
        user_id      INTEGER                 NOT NULL DISTKEY,
        level        VARCHAR(5)              NOT NULL,
        song_id      VARCHAR(18)             NOT NULL,
        artist_id    VARCHAR(256)            NOT NULL,
        session_id   VARCHAR(100)            NOT NULL,
        location     VARCHAR(100)            NULL,
        user_agent   VARCHAR(256)            NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER       NOT NULL SORTKEY,
        first_name  VARCHAR(256)  NULL,
        last_name   VARCHAR(256)  NULL,
        gender      VARCHAR(1)    NULL,
        level       VARCHAR(5)    NULL
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR(18)     NOT NULL SORTKEY,
        title       TEXT            NOT NULL,
        artist_id   VARCHAR(256)    NOT NULL,
        year        INTEGER         NOT NULL DEFAULT(0),
        duration    DECIMAL(10)     NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR(256)   NOT NULL SORTKEY,
        name        VARCHAR(256)   NULL,
        location    VARCHAR(256)   NULL,
        latitude    DECIMAL        NULL,
        longitude   DECIMAL        NULL
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP  NOT NULL SORTKEY,
        hour        SMALLINT   NULL,
        day         SMALLINT   NULL,
        week        SMALLINT   NULL,
        month       SMALLINT   NULL,
        year        SMALLINT   NULL,
        weekday     SMALLINT   NULL
    ) DISTSTYLE ALL;
""")

#
# STAGING TABLES
#
ARN          = config.get('IAM_ROLE', 'ARN')
AWS_REGION   = config.get('AWS', 'REGION', fallback='us-west-2')
LOG_DATA     = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA    = config.get('S3', 'SONG_DATA')

staging_events_copy = f"""
    COPY stagingevents FROM {LOG_DATA}
        CREDENTIALS 'aws_iam_role={ARN}' FORMAT AS JSON {LOG_JSONPATH}
    STATUPDATE ON REGION '{AWS_REGION}';
"""

staging_songs_copy = f"""
    COPY stagingsongs FROM {SONG_DATA}
        CREDENTIALS 'aws_iam_role={ARN}' FORMAT AS JSON 'auto'
    STATUPDATE ON REGION '{AWS_REGION}';
"""

#
# FINAL TABLES
#
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + stagingevents.ts/1000 * INTERVAL '1 second' AS start_time,
        stagingevents.userId                                            AS user_id,
        stagingevents.level                                             AS level,
        stagingsongs.song_id                                            AS song_id,
        stagingsongs.artist_id                                          AS artist_id,
        stagingevents.sessionId                                         AS session_id,
        stagingevents.location                                          AS location,
        stagingevents.userAgent                                         AS user_agent
    FROM stagingevents
    JOIN stagingsongs 
        ON stagingevents.artist = stagingsongs.artist_name
    WHERE
        stagingevents.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        staging.userId     AS user_id,
        staging.firstName  AS first_name,
        staging.lastName   AS last_name,
        staging.gender     AS gender,
        staging.level      AS level
    FROM stagingevents AS staging
    WHERE
        staging.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        staging.song_id     AS song_id,
        staging.title       AS title,
        staging.artist_id   AS artist_id,
        staging.year        AS year,
        staging.duration    AS duration
    FROM stagingsongs AS staging;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        staging.artist_id         AS artist_id,
        staging.artist_name       AS name,
        staging.artist_location   AS location,
        staging.artist_latitude   AS latitude,
        staging.artist_longitude  AS longitude
    FROM stagingsongs AS staging;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + staging.ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour  FROM start_time)   AS hour,
        EXTRACT(day   FROM start_time)   AS day,
        EXTRACT(week  FROM start_time)   AS week,
        EXTRACT(month FROM start_time)   AS month,
        EXTRACT(year  FROM start_time)   AS year,
        EXTRACT(week  FROM start_time)   AS weekday
    FROM stagingevents AS staging
    WHERE 
        staging.page = 'NextSong';
""")

#
# QUERY LISTS
#
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
