# Sparkify Postgres ETL

    This project is the first nanodegree project assignment to test the knowledge on relational data modelling. Below are the objectives for this project -- 
        i) Understanding on relational data model.
        ii) Creating STAR schema.
        iii) ETL Pipeline to load data into postgres db.
    
# Context
   
    A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

    Your role is to create a database schema and ETL pipeline for this analysis.

# Data Sets

   Below are 2 types of data handled by the project --
    
   i) Log Data -- all json files are nested in subdirectories under /data/log_data. A sample of a single row of each files is:
   `{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
`
    
   ii) Song Data --  all json files are nested in subdirectories under /data/song_data. A sample of this files is:
   `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

# Database & Schema

## The dataset will be structured hence we will be using postgres as our database engine. The database schema will be designed as STAR schema to make it analytics friendly. Below will be the tables in it --

   ### Fact Table: There will be 1 fact table called "songplays" and columns of that are --
       `songplay_id (INT) PRIMARY KEY: ID of each user song play
        start_time (VARCHAR) NOT NULL: Timestamp of beggining of user activity
        user_id (INT) NOT NULL: ID of user
       level (TEXT): User level {free | paid}
       song_id (TEXT) NOT NULL: ID of Song played
       artist_id (TEXT) NOT NULL: ID of Artist of the song played
       session_id (INT): ID of the user Session
       location (TEXT): User location
       user_agent (TEXT): Agent used by user to access Sparkify platform`
         
   ### Dimenssion Tables -- 
       a) users - Details of user --
           user_id (INT) PRIMARY KEY: ID of user
           first_name (TEXT) NOT NULL: Name of user
           last_name (TEXT) NOT NULL: Last Name of user
           gender (TEXT): Gender of user {M | F}
           level (TEXT): User level {free | paid}
            
       b) songs - Song details in the music library -- 

          song_id (TEXT) PRIMARY KEY: ID of Song
          title (TEXT) NOT NULL: Title of Song
          artist_id (TEXT) NOT NULL: ID of song Artist
          year (INT): Year of song release
          duration (FLOAT) NOT NULL: Song duration in milliseconds
          
       c) artists - Artist details for the songs in the library --

          artist_id (TEXT) PRIMARY KEY: ID of Artist
          name (TEXT) NOT NULL: Name of Artist
          location (TEXT): Name of Artist city
          lattitude (FLOAT): Lattitude location of artist
          longitude (FLOAT): Longitude location of artist
      
      d) time - Timestamp & broken down details of the date for each record in songplays fact table --

         start_time (DATE) PRIMARY KEY: Timestamp of row
         hour (INT): Hour associated to start_time
         day (INT): Day associated to start_time
         week (INT): Week of year associated to start_time
         month (INT): Month associated to start_time
         year (INT): Year associated to start_time
         weekday (TEXT): Name of week day associated to start_time
         
## ETL Pipeline --

    i) The etl pipeline starts by creating the database & tables as described above.
    ii) The data from songs & logs directory inside data directory is read & processed next.
    iii) First we read the song_data and then extract the data for songs & artists dimenssion tables.
    iv) Once that is done we go for processing log_data. In this step the data is read from log sub-directory of data directory and data is loaded into users, time dimenssion tables & songplays fact table.
    
## Project Structure -- 
    i) data -- Directory that holds the input data for processing.
        a) log_data -- Holds log_data separated by date/hour.
        b) song_data -- Holds song_data separated by tracks.
        
    ii) sql_queries.py -- Holds all the ddl, dml query templates.
    iii) create_table.py -- Utilizes the DDL queries in sql_queries.py and creates the database `sparkify` and all required tables.
    iv) etl.py -- The processing logic for the log & song data is in this file.
    v) etl.ipynb -- Reads & processes single file in song_data & log_data directories.
    vi) test.ipynb -- This file is to test the logic and writing test cases.