import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
  artist             VARCHAR(300),
  auth               VARCHAR(20),
  firstName          VARCHAR(50),
  gender             VARCHAR(5) ,
  itemInSession      INTEGER,
  lastName           VARCHAR(50),
  length             DECIMAL,
  level              VARCHAR(10),
  location           VARCHAR(300),
  method             VARCHAR(10),
  page               VARCHAR(30),
  registration       DECIMAL,
  sessionId          INTEGER,
  song               VARCHAR(200),
  status             SMALLINT,
  ts                 BIGINT,
  userAgent          VARCHAR(200),
  userId             INTEGER
);
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs           INTEGER,
    artist_id           VARCHAR(50),
    artist_latitude     VARCHAR(200),
    artist_longitude    VARCHAR(200),
    artist_location     VARCHAR(300),
    artist_name         VARCHAR(200),
    song_id             VARCHAR(50) NOT NULL PRIMARY KEY,
    title               VARCHAR(200),
    duration            VARCHAR(25),
    year                INTEGER
);
""")

# FACT TABLE

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    start_time       TIMESTAMP,
    user_id          INTEGER,
    level            VARCHAR(10),
    song_id          VARCHAR(50),
    artist_id        VARCHAR(50),
    session_id       INTEGER,
    location         VARCHAR(300),
    user_agent       VARCHAR(200)    
)
""")


# DIMENSION TABLES

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id           INT NOT NULL PRIMARY KEY,
    first_name        VARCHAR(50),
    last_name         VARCHAR(50),
    gender            VARCHAR(5),
    level             VARCHAR(10)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id           VARCHAR(50) NOT NULL PRIMARY KEY,
    title             VARCHAR(200),
    artist_id         VARCHAR(50),
    year              SMALLINT,
    duration          DECIMAL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id         VARCHAR(50) NOT NULL PRIMARY KEY,
    name              VARCHAR(200),
    location          VARCHAR(300),
    latitude          VARCHAR(200),
    longitude         VARCHAR(200)         
    
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time        TIMESTAMP NOT NULL PRIMARY KEY,
    hour              SMALLINT,
    day               SMALLINT,
    week              SMALLINT,
    month             SMALLINT,
    year              SMALLINT,
    weekday           SMALLINT
)
""")


# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
    credentials 'aws_iam_role={}'
    FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('CLUSTER ROLE', 'DWH_ROLE_ARN'),
    config.get('S3', 'LOG_JSONPATH'))


staging_songs_copy = ("""
COPY staging_songs FROM {}
    credentials 'aws_iam_role={}' 
    FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('CLUSTER ROLE', 'DWH_ROLE_ARN')
)

# INSERT DATA IN STAR SCHMEA TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, 
                        location, user_agent)
SELECT 
       DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time,
       se.userId AS user_id,
       se.level AS level,
       ss.song_id AS song_id,
       ss.artist_id AS artist_id,
       se.sessionId AS session_id,
       se.location AS location,
       se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss  ON (se.artist = ss.artist_name AND se.song = ss.title AND se.length = ss.duration)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT  DISTINCT se.userId AS user_id,
        se.firstName AS first_name,
        se.lastName AS last_name,
        se.gender AS gender,
        se.level AS level
FROM staging_events se WHERE se.userId IS NOT NULL 
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT  DISTINCT ss.song_id AS song_id,
        ss.title AS title,
        ss.artist_id AS artist_id,
        ss.year AS year,
        ss.duration AS duration
FROM staging_songs ss WHERE ss.song_id IS NOT NULL 
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT  DISTINCT ss.artist_id AS artist_id,
        ss.artist_name AS name,
        ss.artist_location AS location,
        ss.artist_latitude AS latitude,
        ss.artist_longitude AS longitude
FROM staging_songs ss WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)        
SELECT  DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(dow FROM start_time)
FROM staging_events se WHERE se.userId IS NOT NULL

""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
