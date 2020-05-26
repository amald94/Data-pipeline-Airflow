# Project: Data Pipelines with Airflow

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will create an ETL pipeline using Apache Airflow to build a data warehouses hosted on Redshift.


## Summary
* [Project structure](#Structure)
* [Datasets](#Datasets)
* [Analytics](#Analytics)
* [Schema](#Schema)
* [Execute](#Execute)

## Datasets

You'll be working with two datasets that reside in S3. Here are the S3 links for each:

* <b> Song data </b> - s3://udacity-dend/song_data
* <b> Log data </b> - s3://udacity-dend/log_data

#### Structure
* <b> /images </b> - some screenshots.
* <b> /dags </b> - folder containing dags.
* <b> /plugins </b> - folder containing etl scripts and custom operators. 

## Schema

#### Fact Table
songplays - records in event data associated with song plays. Columns for the table:

    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables 
##### users

    user_id, first_name, last_name, gender, level
##### songs

    song_id, title, artist_id, year, duration

##### artists

    artist_id, name, location, lattitude, longitude

##### time

    start_time, hour, day, week, month, year, weekday

