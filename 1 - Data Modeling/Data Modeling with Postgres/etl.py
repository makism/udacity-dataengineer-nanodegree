""" ETL Pipeline


"""
# author Udacity
# author Avraam Marimpis <makhsm@gmail.com>
# version 1.1 - Add missing docstring.

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ Process a song (JSON) record.
    
    This functions loads the given json file (``filepath``), extracts the
    fields relevant a _song_ and an _artist_ and stores them in the appropriate
    tables.
    
    Parameters
    ----------
    cur: psycopg2.cursor
        A cursor object ("connection") to a database.
    
    filepath: string
        A complete path to a JSON file.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # select relevant fields for the "song" table
    #artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year = df.values[0]
    #artist_id, _, _, _, _, duration, _, song_id, title, year = df.values[0]
    song_fields = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = df[song_fields].values.tolist()[0]
    # insert song record
    cur.execute(song_table_insert, song_data)
    
    # select relevant fields for the "artist" table
    #artist_id, artist_latitude, artist_location, artist_longitude, artist_name, *_ = df.values[0]
    #artist_data = [artist_id, artist_name, artist_location, artist_latitude, artist_longitude]
    artists_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artist_data = df[artists_fields].values.tolist()[0]
    # insert artist record
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ Process a log (JSON) file, extract and store information and metadata for listening/song plays.
    
    This functions involves multiple processing steps. Please consult the file `README.md` for more details.
    
    Parameters
    ----------
    cur: psycopg2.cursor
        A cursor object ("connection") to a database.
    
    filepath: string
        A complete path to a JSON file.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]
    
    # convert timestamp column to datetime
    t = df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = []
    for row in t:
        new_repr = [row, row.hour, row.day, row.weekofyear, row.month, row.year, row.weekday()]
        time_data.append(new_repr)
    
    column_labels = ["timestamp", "hour", "day", "week of year", "month", "year", "weekday"]
    
    time_df = pd.DataFrame(data=time_data, columns=column_labels)
    time_df.sort_values(by='timestamp', ascending=True, inplace=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
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
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ Process data
    
    Parameters
    ----------
    cur: psycopg2.cursor
        A cursor object ("connection") to a database.
    
    conn: psycopg2.connection
        A connection to our database.
    
    func: method
        A user-defined-function; in our case it's one of the methods we developed `process_song_file` or `process_log_file`.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()