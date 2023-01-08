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
    sp_start_time       TIMESTAMP,
    sp_user_id          INTEGER,
    sp_level            VARCHAR(10),
    sp_song_id          VARCHAR(50),
    sp_artist_id        VARCHAR(50),
    sp_session_id       INTEGER,
    sp_location         VARCHAR(300),
    sp_user_agent       VARCHAR(200)    
)
""")


# DIMENSION TABLES

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    u_user_id           INT NOT NULL PRIMARY KEY,
    u_first_name        VARCHAR(50),
    u_last_name         VARCHAR(50),
    u_gender            VARCHAR(5),
    u_level             VARCHAR(10)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    s_song_id           VARCHAR(50) NOT NULL PRIMARY KEY,
    s_title             VARCHAR(200),
    s_artist_id         VARCHAR(50),
    s_year              SMALLINT,
    s_duration          DECIMAL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    a_artist_id         VARCHAR(50) NOT NULL PRIMARY KEY,
    a_name              VARCHAR(200),
    a_location          VARCHAR(300),
    a_latitude          VARCHAR(200),
    a_longitude         VARCHAR(200)         
    
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    t_start_time        TIMESTAMP NOT NULL PRIMARY KEY,
    t_hour              SMALLINT,
    t_day               SMALLINT,
    t_week              SMALLINT,
    t_month             SMALLINT,
    t_year              SMALLINT,
    t_weekday           SMALLINT
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
INSERT INTO songplays (sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, 
                        sp_location, sp_user_agent)
SELECT 
       DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS sp_start_time,
       se.userId AS sp_user_id,
       se.level AS sp_level,
       ss.song_id AS sp_song_id,
       ss.artist_id AS sp_artist_id,
       se.sessionId AS sp_session_id,
       se.location AS sp_location,
       se.userAgent AS sp_user_agent
FROM staging_events se
JOIN staging_songs ss  ON (se.artist = ss.artist_name AND se.song = ss.title AND se.length = ss.duration)
""")

user_table_insert = ("""
INSERT INTO users (u_user_id, u_first_name, u_last_name, u_gender, u_level)
SELECT  DISTINCT se.userId AS u_user_id,
        se.firstName AS u_first_name,
        se.lastName AS u_last_name,
        se.gender AS u_gender,
        se.level AS u_level
FROM staging_events se WHERE se.userId IS NOT NULL 
""")

song_table_insert = ("""
INSERT INTO songs (s_song_id, s_title, s_artist_id, s_year, s_duration)
SELECT  DISTINCT ss.song_id AS s_song_id,
        ss.title AS s_title,
        ss.artist_id AS s_artist_id,
        ss.year AS s_year,
        ss.duration AS s_duration
FROM staging_songs ss WHERE ss.song_id IS NOT NULL 
""")

artist_table_insert = ("""
INSERT INTO artists (a_artist_id, a_name, a_location, a_latitude, a_longitude)
SELECT  DISTINCT ss.artist_id AS a_artist_id,
        ss.artist_name AS a_name,
        ss.artist_location AS a_location,
        ss.artist_latitude AS a_latitude,
        ss.artist_longitude AS a_longitude
FROM staging_songs ss WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday)        
SELECT  DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS t_start_time,
        EXTRACT(hour FROM t_start_time),
        EXTRACT(day FROM t_start_time),
        EXTRACT(week FROM t_start_time),
        EXTRACT(month FROM t_start_time),
        EXTRACT(year FROM t_start_time),
        EXTRACT(dow FROM t_start_time)
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
