import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIMES"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS STAGING_EVENTS
(
  artist             VARCHAR(50) NOT NULL,
  auth               VARCHAR(20) NOT NULL,
  firstName          VARCHAR(50) NOT NULL,
  gender             VARCHAR(5) NOT NULL,
  itemInSession      INTEGER NOT NULL,
  lastName           VARCHAR(50) NOT NULL,
  length             VARCHAR(25) NOT NULL,
  level              VARCHAR(10) NOT NULL,
  location           VARCHAR(50) NOT NULL,
  method             VARCHAR(10) NOT NULL,
  page               VARCHAR(10) NOT NULL,
  registration       DECIMAL NOT NULL,
  sessionId          INT NOT NULL,
  song               VARCHAR(50) NOT NULL,
  status             SMALLINT NOT NULL,
  ts                 BIGINT NOT NULL,
  userAgent          VARCHAR(100) NOT NULL,
  userId             INT NOT NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS STAGING_SONGS
(
    num_songs           INT NOT NULL,
    artist_id           VARCHAR(20) NOT NULL,
    artist_latitude     VARCHAR(20),
    artist_longitude    VARCHAR(20),
    artist_location     VARCHAR(20),
    artist_name         VARCHAR(50) NOT NULL,
    song_id             VARCHAR(20) NOT NULL PRIMARY KEY,
    title               VARCHAR(50) NOT NULL,
    duration            VARCHAR(25) NOT NULL,
    year                SMALLINT
);
""")

# FACT TABLE

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGPLAYS 
(
    sp_songplay_id      SERIAL NOT NULL PRIMARY KEY,
    sp_start_time       TIMESTAMP NOT NULL,
    sp_user_id          INT NOT NULL,
    sp_level            VARCHAR(10) NOT NULL,
    sp_song_id          VARCHAR(20) NOT NULL,
    sp_artist_id        VARCHAR(20) NOT NULL,
    sp_session_id       INT NOT NULL,
    sp_location         VARCHAR(50) NOT NULL,
    sp_user_agent       VARCHAR(100)    
)
""")

# DIMENSION TABLES

user_table_create = ("""
CREATE TABLE IF NOT EXISTS USERS
(
    u_user_id           INT NOT NULL PRIMARY KEY,
    u_first_name        VARCHAR(50) NOT NULL,
    u_last_name         VARCHAR(50) NOT NULL,
    u_gender            VARCHAR(5) NOT NULL,
    u_level             VARCHAR(10) NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGS
(
    s_song_id           VARCHAR(20) NOT NULL PRIMARY KEY,
    s_title             VARCHAR(30) NOT NULL,
    s_artist_id         VARCHAR(20) NOT NULL,
    s_year              SMALLINT,
    s_duration          DECIMAL NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS ARTISTS
(
    a_artist_id         VARCHAR(20) NOT NULL PRIMARY KEY,
    a_name              VARCHAR(50) NOT NULL,
    a_location          VARCHAR(20),
    a_latitude          VARCHAR(20),
    a_longitude         VARCHAR(20)         
    
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS TIMES
(
    t_start_time        TIMESTAMP NOT NULL PRIMARY KEY,
    t_hour              SMALLINT NOT NULL,
    t_day               SMALLINT NOT NULL,
    t_week              SMALLINT NOT NULL,
    t_month             SMALLINT NOT NULL,
    t_year              SMALLINT NOT NULL,
    t_weekday           SMALLINT NOT NULL
)
""")


# STAGING TABLES

staging_events_copy = ("""

""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO SONGPLAYS (sp_songplay_id, sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, 
                        sp_location, sp_user_agent)
SELECT default AS songplay_id,
       TO_TIMESTAMP(se.ts) AS sp_start_time,
       se.userId AS sp_user_id,
       se.level AS sp_level,
       ss.song_id AS sp_song_id,
       ss.artist_id AS sp_artist_id,
       se.sessionId AS sp_session_id,
       se.location AS sp_location,
       se.userAgent AS sp_user_agent,
FROM STAGING_EVENTS se
JOIN STAGING_SONGS ss  ON (se.artist = ss.artist_name AND se.song = ss.title AND se.length = ss.duration)
""")

user_table_insert = ("""
INSERT INTO USERS (u_user_id, u_first_name, u_last_name, u_gender, u_level)
SELECT  se.userId AS u_user_id,
        se.firstName AS u_first_name,
        se.lastName AS u_last_name,
        se.gender AS u_gender,
        se.level AS u_level
FROM STAGING_EVENTS se
""")

song_table_insert = ("""
INSERT INTO SONGS (s_song_id, s_title, s_artist_id, s_year, s_duration)
SELECT  ss.song_id AS s_song_id,
        ss.title AS s_title,
        ss.artist_id AS s_artist_id,
        ss.year AS s_year,
        ss.duration AS s_duration
FROM STAGING_SONGS ss
""")

artist_table_insert = ("""
INSERT INTO ARTISTS (a_artist_id, a_name, a_location, a_latitude, a_longitude)
SELECT  ss.artist_id AS a_artist_id,
        ss.artist_name AS a_name,
        ss.artist_location AS a_location,
        ss.artist_latitude AS a_latitude,
        ss.artist_longitude AS a_longitude
FROM STAGING_SONGS ss
""")

time_table_insert = ("""
INSERT INTO TIMES (t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday)        
SELECT  DISTINCT(TO_TIMESTAMP(se.ts) AS t_start_time,
        TO_CHAR(TO_TIMESTAMP(se.ts), 'HH24') AS t_hour,
        TO_CHAR(TO_TIMESTAMP(se.ts), 'DD') AS t_day,
        TO_CHAR(TO_TIMESTAMP(se.ts), 'WW') AS t_week,
        TO_CHAR(TO_TIMESTAMP(se.ts), 'MM') AS t_month,
        TO_CHAR(TO_TIMESTAMP(se.ts), 'YYYY') AS t_year,
        TO_CHAR(TO_TIMESTAMP(se.ts), 'ID') AS t_weekday
FROM STAGING_EVENTS se

""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
