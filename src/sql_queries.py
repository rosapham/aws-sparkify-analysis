import configparser

# Get the configuration values from the dwh.cfg file
config = configparser.ConfigParser()
config.read("./dwh.cfg")

SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")


# DROP ALL TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# Create the staging_events table
staging_events_table_create = """
    CREATE TABLE staging_events (
    artist     	    varchar,
    auth        	varchar,
    firstName     	varchar,
    gender        	varchar,
    itemInSession   integer,
    lastName      	varchar,
    length       	varchar,
    level        	varchar,
    location    	varchar,
    method     	    varchar,
    page        	varchar,
    registration    varchar,
    sessionId      	integer,
    song        	varchar,
    status      	integer,
    ts          	bigint,
    userAgent      	varchar,
    userId      	integer
    );
"""

# Create the staging_songs table
staging_songs_table_create = """
    CREATE TABLE staging_songs (
        num_songs          integer,
        artist_id          varchar,
        artist_latitude    float,
        artist_longitude   float,
        artist_location    varchar,
        artist_name        varchar,
        song_id            varchar,
        title              varchar,
        duration           float,
        year               integer
    );
"""

# Create the fact songplays table
songplay_table_create = """
    CREATE TABLE songplays (
        songplay_id    integer identity(0,1) primary key,
        start_time     timestamp sortkey distkey,
        user_id        integer,
        level          varchar,
        song_id        varchar,
        artist_id      varchar,
        session_id     integer,
        location       varchar,
        user_agent     varchar
    );
"""

# Create the dimension users table
user_table_create = """
    CREATE TABLE users (
        user_id     integer primary key sortkey,
        first_name  varchar,
        last_name   varchar,
        gender      varchar,
        level       varchar
    );
"""

# Create the dimension songs table
song_table_create = """
    CREATE TABLE songs (
        song_id     varchar primary key sortkey,
        title       varchar,
        artist_id   varchar,
        year        integer,
        duration    float
    );
"""

# Create the dimension artists table
artist_table_create = """
    CREATE TABLE artists (
        artist_id   varchar primary key sortkey,
        name        varchar,
        location    varchar,
        latitude    float,
        longitude   float
    );
"""

# Create the dimension time table
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

# COPY DATA FROM S3 INTO STAGING TABLES

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

# INSERT VALUES INTO THE FACT AND DIMENSION TABLES

songplay_table_insert = """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id,
            location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
              userId as user_id,
              level,
              song_id,
              artist_id,
              sessionId as session_id,
              location,
              userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss
    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE page = 'NextSong' AND userId IS NOT NULL AND song_id IS NOT NULL AND artist_id IS NOT NULL
        AND ts IS NOT NULL
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
              firstName as first_name,
              lastName as last_name,
              gender,
              level
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL
"""

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
              title,
              artist_id,
              year,
              duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
              artist_name as name,
              artist_location as location,
              artist_latitude as latitude,
              artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
"""

time_table_insert = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
              EXTRACT(hour FROM start_time) as hour,
              EXTRACT(day FROM start_time) as day,
              EXTRACT(week FROM start_time) as week,
              EXTRACT(month FROM start_time) as month,
              EXTRACT(year FROM start_time) as year,
              EXTRACT(weekday FROM start_time) as weekday
    FROM staging_events
    WHERE page = 'NextSong' AND ts IS NOT NULL
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
