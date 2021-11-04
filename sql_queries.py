import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE staging_events (

    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (

    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id IDENTITY(0, 1) PRIMARY KEY, 
        start_time timestamp NOT NULL, 
        user_id integer NOT NULL, 
        level varchar(), 
        song_id varchar(), 
        artist_id varchar(),
        session_id integer, 
        location varchar(), 
        user_agent varchar()
    );
""")

user_table_create = ("""
    CREATE TABLE user (
        user_id int PRIMARY KEY, 
        first_name varchar(), 
        last_name varchar(), 
        gender varchar(), 
        level varchar()
    );
""")

song_table_create = ("""
    CREATE TABLE song (
        song_id varchar() PRIMARY KEY, 
        title varchar(), 
        artist_id varchar(), 
        year integer, 
        duration float
    );
""")

artist_table_create = ("""
    CREATE TABLE artist (
        artist_id varchar() PRIMARY KEY, 
        name varchar() NOT NULL, 
        location varchar(), 
        latitude varchar(), 
        longitude varchar()
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        time_id IDENTITY(0, 1) PRIMARY KEY,
        timestamp timestamp,
        hour integer, 
        day integer, 
        week integer, 
        month integer, 
        year integer, 
        weekday integer
    )
""")

# STAGING TABLES
staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES
songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
