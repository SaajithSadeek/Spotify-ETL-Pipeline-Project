create or replace database spotify_de;

create or replace schema main_schema;

//////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////

// Create 3 tables for the 3 source file folders in s3

create or replace table spotify_de.main_schema.album (
    album_id STRING PRIMARY KEY,
    name STRING,
    release_date DATE,
    total_track INT,
    url STRING
);

select * from spotify_de.main_schema.album;

create or replace table spotify_de.main_schema.artist (
    artist_id STRING PRIMARY KEY,
    artist_name STRING,
    external_url STRING
);

select * from spotify_de.main_schema.artist;

create or replace table spotify_de.main_schema.song (
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING,
    CONSTRAINT fk_album FOREIGN KEY (album_id) REFERENCES spotify_de.main_schema.album(album_id),
    CONSTRAINT fk_artist FOREIGN KEY (artist_id) REFERENCES spotify_de.main_schema.artist(artist_id)
    
);

select * from spotify_de.main_schema.song;

CREATE OR REPLACE FILE FORMAT spotify_de.main_schema.csv_file_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY='"';

//////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////
// Create 3 stages

CREATE STAGE spotify_de.main_schema.album
  URL='s3://spotify-etl-project-saajith/transformed_data/album_data/'
  CREDENTIALS=(AWS_KEY_ID='INCLUDE_YOUR_AWS_KEY_ID' AWS_SECRET_KEY='INCLUDE_YOUR_AWS_SECRET_KEY');

LIST @spotify_de.main_schema.album;

CREATE STAGE spotify_de.main_schema.artist
  URL='s3://spotify-etl-project-saajith/transformed_data/artist_data/'
  CREDENTIALS=(AWS_KEY_ID='INCLUDE_YOUR_AWS_KEY_ID' AWS_SECRET_KEY='INCLUDE_YOUR_AWS_SECRET_KEY');

LIST @spotify_de.main_schema.artist;

CREATE STAGE spotify_de.main_schema.song
  URL='s3://spotify-etl-project-saajith/transformed_data/songs_data/'
  CREDENTIALS=(AWS_KEY_ID='INCLUDE_YOUR_AWS_KEY_ID' AWS_SECRET_KEY='INCLUDE_YOUR_AWS_SECRET_KEY');

LIST @spotify_de.main_schema.song;

//////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////
// Create three pipes for each table

// album pipe, PS: Copy the notification_channel from the pipe and add it to the respect event notification in s3

CREATE PIPE spotify_de.main_schema.album_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_de.main_schema.album
FROM @spotify_de.main_schema.album
ON_ERROR = 'CONTINUE';

select * from spotify_de.main_schema.album;

desc pipe spotify_de.main_schema.album_pipe;

// artist pipe, PS: Copy the notification_channel from the pipe and add it to the respect event notification in s3

CREATE PIPE spotify_de.main_schema.artist_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_de.main_schema.artist
FROM @spotify_de.main_schema.artist
ON_ERROR = 'CONTINUE';

select * from spotify_de.main_schema.artist;

desc pipe spotify_de.main_schema.artist_pipe;

// song pipe, PS: Copy the notification_channel from the pipe and add it to the respect event notification in s3

CREATE PIPE spotify_de.main_schema.song_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_de.main_schema.song
FROM @spotify_de.main_schema.song
ON_ERROR = 'CONTINUE';

select * from spotify_de.main_schema.song;

desc pipe spotify_de.main_schema.song_pipe;

/// Run Query to test whether it gets copied automatically

select * from spotify_de.main_schema.album;
select * from spotify_de.main_schema.artist;
select * from spotify_de.main_schema.song;