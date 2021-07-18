import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import io

def process_song_file(cur, filepath):
    
    """
        This function reads json data related to songs. 
        Uploads into songs & artists table ofpostgres db.
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # Extract only values for the songs data
    song_data = df[["song_id","title","artist_id","year","duration"]].values.tolist()
    
    # Insert all rows into songs table. Executemany is used for improved performance.
    cur.executemany(song_table_insert, song_data)
    
    #Extract only the values for the artist data
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values.tolist()
    
    # Insert all rows into artists table. Executemany is used for improved performance.
    cur.executemany(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
        This function reads json data related to logs stored. 
        Uploads data into users, time & songplays tables of postgres db.
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"],unit="ms")
    
    # Extract all required data e.g day, week, month from the extracted ts field.
    time_data = [[time, time.hour, time.day, time.week, time.month, time.year, time.day_name()] for time in t] 
    
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    
    # Create dataframe from the list along with headers
    time_df = pd.DataFrame(time_data, columns=column_labels)
    
    # Loop through each row & insert into time table.
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Extract required data from dataframe for user table
    user_df = df[["userId","firstName","lastName","gender","level"]]
        
    # Loop through each row and insert into users table
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

        # Create songplay records
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        
        # Insert into songplays table
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    
    """
        This function reads data recusively from a folder and passes on to individual function for processing.
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
    
    """
        The main function is responsible for kickstarting the entire code.
        This will acquire connection to postgres db.
        Processes song & log data by calling appropriate function.
    """
    
    # Acquiring connection to postgres
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # Calling process_data for song_data
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    # Calling process_data for log_data
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # Closing the connection to db.
    conn.close()


if __name__ == "__main__":
    
    # Starting point of processing.
    main()