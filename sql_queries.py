import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_data"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_data"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_log_data(
    artist TEXT,
    auth TEXT,
    gender TEXT,
    first_name TEXT,
    item_in_session INTEGER,
    last_name TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    regestration TEXT,
    session_id INTEGER,
    song TEXT,
    status INTEGER,
    ts BIGINT,
    user_agent TEXT,
    user_id INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_song_data(
        song_id TEXT,
        num_songs INTEGER,
        artist_id TEXT,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_name TEXT,
        title TEXT,
        duration FLOAT,
        year Integer,
        artist_location TEXT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INTEGER NOT NULL DISTKEY,
    level TEXT, 
    song_id TEXT, 
    artist_id TEXT, 
    session_id INTEGER, 
    location TEXT, 
    user_agent TEXT)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY Distkey, 
    first_name TEXT,
    last_name TEXT, 
    gender TEXT,
    level TEXT
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song(
        song_id TEXT PRIMARY KEY SORTKEY,
        title TEXT,
        artist_id TEXT distkey,
        year Integer,
        duration FLOAT
    ) 
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist(
    artist_id TEXT PRIMARY KEY SORTKEY,
    name TEXT, 
    location TEXT,
    lattitude FLOAT, 
    longitude FLOAT 
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY SORTKEY, 
    hour Integer, 
    day Integer, 
    week Integer, 
    month Integer, 
    year Integer distkey, 
    weekday Integer
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    iam_role '{}'
    FORMAT as json {};
    """).format( 
    "staging_log_data",
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY {} FROM {}
    iam_role '{}'
    FORMAT as json 'auto';
    """).format( 
    "staging_song_data",
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
 INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
 SELECT
        TIMESTAMP 'epoch' + (l.ts/1000 * INTERVAL '1 second') as start_time,
        l.user_id,
        l.level,
        s.song_id,
        s.artist_id,
        l.session_id,
        l.location,
        l.user_agent
    FROM staging_log_data l
    JOIN staging_song_data s ON
        l.song = s.title AND
        l.artist = s.artist_name
     WHERE
        l.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_log_data
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO song
SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_song_data
""")

artist_table_insert = ("""
INSERT INTO artist
SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_song_data
""")

time_table_insert = ("""
INSERT INTO time
SELECT distinct start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
