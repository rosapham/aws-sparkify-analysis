import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("./dwh.cfg")

SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE staging_events (
    artist     	    varchar(100),
    auth        	varchar(25),
    firstName     	varchar(25),
    gender        	varchar(1),
    itemInSession   integer not null,
    lastName      	varchar(25),
    length       	varchar(15),
    level        	varchar(10),
    location    	varchar(50),
    method     	    varchar(10),
    page        	varchar(25),
    registration    integer,
    sessionId      	integer not null,
    song        	varchar(100),
    status      	integer,
    ts          	integer,
    userAgent      	varchar(150),
    userId      	integer not null
    );
"""

staging_songs_table_create = """
    CREATE TABLE staging_songs (
        num_songs          integer,
        artist_id          varchar(100) not null,
        artist_latitude    float,
        artist_longitude   float,
        artist_location    varchar(100),
        artist_name        varchar(100),
        song_id            varchar(100) not null,
        title              varchar(100),
        duration           float,
        year               integer
    );
"""

songplay_table_create = """
    CREATE TABLE songplays (
        songplay_id    integer identity(0,1) primary key,
        start_time     timestamp not null sortkey distkey,
        user_id        integer not null,
        level          varchar(10),
        song_id        varchar(100) not null,
        artist_id      varchar(100) not null,
        session_id     integer not null,
        location       varchar(50),
        user_agent     varchar(150)
    );
"""

user_table_create = """
    CREATE TABLE users (
        user_id     integer primary key sortkey,
        first_name  varchar(25),
        last_name   varchar(25),
        gender      varchar(1),
        level       varchar(10)
    );
"""

song_table_create = """
    CREATE TABLE songs (
        song_id     varchar(100) primary key sortkey,
        title       varchar(100),
        artist_id   varchar(100) not null,
        year        integer,
        duration    float
    );
"""

artist_table_create = """
    CREATE TABLE artists (
        artist_id   varchar(100) primary key sortkey,
        name        varchar(100),
        location    varchar(100),
        latitude    float,
        longitude   float
    );
"""

time_table_create = """
    CREATE TABLE time (
        start_time  timestamp primary key sortkey,
        hour        integer,
        day         integer,
        week        integer,
        month       integer,
        year        integer,
        weekday     integer
    );
"""

# STAGING TABLES

staging_events_copy = (
    """
"""
).format()

staging_songs_copy = (
    """
"""
).format()

# FINAL TABLES

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""

time_table_insert = """
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
