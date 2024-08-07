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
    artist     	    varchar,
    auth        	varchar,
    firstName     	varchar,
    gender        	varchar,
    itemInSession   integer not null,
    lastName      	varchar,
    length       	varchar,
    level        	varchar,
    location    	varchar,
    method     	    varchar,
    page        	varchar,
    registration    varchar,
    sessionId      	integer not null,
    song        	varchar(100),
    status      	integer,
    ts          	bigint,
    userAgent      	varchar(150),
    userId      	integer
    );
"""

staging_songs_table_create = """
    CREATE TABLE staging_songs (
        num_songs          integer,
        artist_id          varchar not null,
        artist_latitude    float,
        artist_longitude   float,
        artist_location    varchar,
        artist_name        varchar,
        song_id            varchar not null,
        title              varchar,
        duration           float,
        year               integer
    );
"""

songplay_table_create = """
    CREATE TABLE songplays (
        songplay_id    integer identity(0,1) primary key,
        start_time     timestamp not null sortkey distkey,
        user_id        integer not null,
        level          varchar,
        song_id        varchar not null,
        artist_id      varchar not null,
        session_id     integer not null,
        location       varchar,
        user_agent     varchar
    );
"""

user_table_create = """
    CREATE TABLE users (
        user_id     integer primary key sortkey,
        first_name  varchar,
        last_name   varchar,
        gender      varchar,
        level       varchar
    );
"""

song_table_create = """
    CREATE TABLE songs (
        song_id     varchar primary key sortkey,
        title       varchar,
        artist_id   varchar not null,
        year        integer,
        duration    float
    );
"""

artist_table_create = """
    CREATE TABLE artists (
        artist_id   varchar primary key sortkey,
        name        varchar,
        location    varchar,
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
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json {}
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
"""
).format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = (
    """
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
"""
).format(SONG_DATA, DWH_ROLE_ARN)

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
