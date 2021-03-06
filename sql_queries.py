import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar(400),
        auth varchar(255),
        firstName varchar(255),
        gender varchar(255),
        itemInSession integer,
        lastName varchar(255),
        length float,
        level varchar(255),
        location varchar(400),
        method varchar(255),
        page varchar(255),
        registration float,
        sessionId integer,
        song varchar(255),
        status integer,
        ts bigint,
        userAgent varchar(255),
        userId integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs integer NOT NULL,
        artist_id varchar(255) NOT NULL,
        artist_location varchar(400), 
        artist_latitude varchar(255), 
        artist_longitude varchar(255),
        artist_name varchar(400),
        song_id varchar(255),
        title varchar(255),
        duration float,
        year integer
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id integer IDENTITY(0, 1) PRIMARY KEY DISTKEY, 
        start_time timestamp NOT NULL, 
        user_id integer NOT NULL, 
        level varchar(255), 
        song_id varchar(255), 
        artist_id varchar(255),
        session_id integer, 
        location varchar(400), 
        user_agent varchar(255)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer PRIMARY KEY DISTKEY, 
        first_name varchar(255), 
        last_name varchar(255), 
        gender varchar(255), 
        level varchar(255) SORTKEY
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id varchar(255) PRIMARY KEY DISTKEY, 
        title varchar(255), 
        artist_id varchar(255), 
        year integer, 
        duration float
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id varchar(255) PRIMARY KEY DISTKEY, 
        name varchar(400) NOT NULL, 
        location varchar(400), 
        latitude varchar(255), 
        longitude varchar(255)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        time_id integer IDENTITY(0, 1) PRIMARY KEY DISTKEY,
        timestamp timestamp SORTKEY,
        hour integer, 
        day integer, 
        week integer, 
        month integer, 
        year integer, 
        weekday integer
    );
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json {}
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time,
        se.userId as user_id,
        se.level as level,
        ss.song_id as song_id,
        ss.artist_id as artist_id,
        se.sessionId as session_id,
        se.location as location,
        se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON ss.title = se.song AND ss.artist_name = se.artist
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender as gender,
        level as level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id as song_id,
        title as title,
        artist_id as artist_id,
        year as year,
        duration as duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id as artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (timestamp, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as timestamp,
        DATE_PART(h, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as hour,
        DATE_PART(d, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as day,
        DATE_PART(w, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as week,
        DATE_PART(mon, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as month,
        DATE_PART(y, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as year,
        DATE_PART(dow, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as weekday
    FROM staging_events;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
