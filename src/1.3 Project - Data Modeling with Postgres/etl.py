import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = (str(df.song_id[0]), str(df.title[0]), str(df.artist_id[0]), int(df.year[0]), float(df.duration[0]))
    print(song_data)
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = (str(df.artist_id[0]), str(df.artist_name[0]), str(df.artist_location[0]), df.artist_latitude[0], df.artist_longitude[0])
    print(artist_data)
    cur.execute(artist_table_insert, artist_data)

    # Currently inserts 'NaN' when no value for latitude and longitude provided (=null in JSON)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    #df.head()

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # insert time data records
    # time_data =
    # column_labels =
    # time_df =

    timestamps = list(df.ts)
    hours = list(t.dt.hour.values)
    days = list(t.dt.day.values)
    weeks = list(t.dt.isocalendar().week.values)
    months = list(t.dt.month.values)
    years = list(t.dt.year.values)
    weekdays = list(t.dt.weekday.values)
    column_labels = ('datetime', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(list(zip(timestamps, hours, days, weeks, months, years, weekdays)), columns=column_labels)
    #time_df.head()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    #print(user_df)

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId,
                         row.location, row.userAgent)
        #print(songplay_data)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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