class SqlQueries:
    
    songplay_table_insert = ("""
        INSERT INTO songplays (
       start_time, 
       user_id, 
       level, 
       song_id, 
       artist_id, 
       session_id, 
       location, 
       user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   
    ON e.song = s.title 
    AND e.artist = s.artist_name;
    """)

    user_table_insert = ("""
       INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL;
    """)

    song_table_insert = ("""
       INSERT INTO songs (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;

    """)

    artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, name, 
        location, 
        latitude, 
        longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    """)

    time_table_insert = ("""
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays;
    """)
