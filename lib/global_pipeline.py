from datetime import datetime, timedelta
from simple_chalk import chalk
from .functions import make_archive
from .Time import Time
from .env import LOCAL_DOWNLOAD_FOLDER
from .Db import Db
from .Sftp import Sftp
import shutil
import pandas as pd
import os
from pandas.errors import EmptyDataError


# Vars
NIELSEN_GLOBAL_FILES_LOCATION = '/' # Location on the remote global servers where the files exist
GLOBAL_ARCHIVE_FOLDER = './global_archive'
GLOBAL_ARCHIVE_DAILY_FOLDER_TEMPLATE = os.path.join(GLOBAL_ARCHIVE_FOLDER, 'global_{}')
GLOBAL_ARCHIVE_DAILY_ZIP_FOLDER_TEMPLATE = os.path.join(GLOBAL_ARCHIVE_FOLDER, 'global_{}')

GLOBAL_COUNTRY_NAMES = {
    'eu_daily': [ 'Austria', 'Belgium', 'Croatia', 'Czech_Republic', 'Denmark', 'Finland', 'France', 'Germany', 'Iceland', 'Ireland', 'Italy', 'Luxembourg', 'Netherlands', 'Norway', 'Poland', 'Portugal', 'Spain', 'Sweden', 'Switzerland', 'United_Kingdom' ],
    'ne_asia_daily': [ 'Japan', 'Korea' ],
    'se_asia_daily': [ 'Australia', 'Hong_Kong', 'Indonesia', 'Malaysia', 'New_Zealand', 'Philippines', 'Singapore', 'Taiwan', 'Thailand', 'Vietnam' ],
    'lat_am_daily': [ 'Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Mexico', 'Peru' ],
    'emerging_daily': [ 'Greece', 'Hungary', 'India', 'Romania', 'Slovakia', 'South_Africa', 'Turkey' ]
}

GLOBAL_SERVER_NAMES = [
    'eu_daily',
    'ne_asia_daily',
    'se_asia_daily',
    'lat_am_daily',
    'emerging_daily'
]

# Templates for generating filenames
ISRC_NAME_TEMPLATE = '{}_Daily_Top100k_ISRCs_{}.tsv'
SONG_NAME_TEMPLATE = '{}_Daily_Top50k_Songs_{}.tsv'
ARTIST_NAME_TEMPLATE = '{}_Daily_Top20k_Artists_{}.tsv'

def cleanArtists(df, date):

    # Drop unnecessary columns
    drop_artist_columns = [
        'Country Code',
        'Country',
        'WeekID',
        'Date',
        'Rank',
        'Digital Song Sales - Current Day',
        'Digital Song Sales - % Change Prior Day',
        'Digital Song Sales - Prior Day',
        'Digital Song Sales - YTD',
        'Digital Song Sales - ATD 2022',
        'Streaming On-Demand Total - % Change Prior Day',
        'Streaming On-Demand Total - YTD',
        'Streaming On-Demand Total - ATD 2022'
    ]

    df = df.drop(columns=drop_artist_columns)

    # Standardize columns
    current_day = datetime.strftime(date, '%Y-%m-%d')
    previous_day = datetime.strftime(date - timedelta(1), '%Y-%m-%d')

    rename_artist_columns = {
        'UnifiedArtistID': 'unified_artist_id',
        'Artist': 'artist',
        'Streaming On-Demand Total - Current Day': current_day,
        'Streaming On-Demand Total - Prior Day': previous_day
    }

    df = df.rename(columns=rename_artist_columns)

    # Separate into meta and streaming info
    meta_columns = [ 'unified_artist_id', 'artist' ]
    streams_columns = [ 'unified_artist_id', current_day, previous_day ]

    meta = df[meta_columns].drop_duplicates(subset='unified_artist_id')
    streams = df[streams_columns].melt(id_vars='unified_artist_id', var_name='date', value_name='streams')

    return meta, streams

def artistsDbUpdates(db, meta, streams, date, country_name):

    # Create temporary tables
    string = """
        create temp table tmp_meta (
            unified_artist_id text,
            artist text
        );

        create temp table tmp_streams (
            unified_artist_id text,
            date date,
            streams int
        );
    """
    db.execute(string)

    # Fill temporary tables
    db.big_insert(meta, 'tmp_meta')
    db.big_insert(streams, 'tmp_streams')

    # Insert metadata
    string = """
        create temp table new_artists as (
            select
                tm.unified_artist_id,
                tm.artist,
                true as is_global
            from tmp_meta tm
            left join nielsen_artist.meta m on tm.unified_artist_id = m.unified_artist_id
            where m.unified_artist_id is null
        );

        insert into nielsen_artist.meta (unified_artist_id, artist, is_global)
        select unified_artist_id, artist, is_global from new_artists;

        select count(*) from new_artists;
    """
    new_artists = db.execute(string)
    num_new_artists = new_artists.loc[0, 'count']
    print(f'Inserted {num_new_artists} new artists')

    # Streams updates
    string = f"""
        create temp table streams as (
            select
                m.id as artist_id,
                ts.date,
                ts.streams,
                case
                    when existing_streams.artist_id is null then false
                    else true
                end as record_exists,
                existing_streams.{country_name} as existing_streams
            from tmp_streams ts
            left join nielsen_artist.meta m on ts.unified_artist_id = m.unified_artist_id
            left join nielsen_artist.streams existing_streams on m.id = existing_streams.artist_id and ts.date = existing_streams.date
        );

        create temp table updates as (
            select artist_id, date, streams
            from streams
            where record_exists is true
                and streams is distinct from existing_streams
        );

        create temp table inserts as (
            select artist_id, date, streams
            from streams
            where record_exists is false
        );

        -- Update existing records
        update nielsen_artist.streams s
        set {country_name} = updates.streams
        from updates
        where s.artist_id = updates.artist_id
            and s.date::date = updates.date::date;

        -- Insert new records
        insert into nielsen_artist.streams (artist_id, date, {country_name})
        select artist_id, date, streams from inserts;

        select count(*) as value, 'updates' as name from updates
        union all
        select count(*) as value, 'inserts' as name from inserts
    """
    params = { 'date': date }
    results = db.execute(string, params)

    num_inserts = results.loc[results['name'] == 'inserts', 'value'].iloc[0]
    num_updates = results.loc[results['name'] == 'updates', 'value'].iloc[0]
    print(f'{num_inserts} inserts | {num_updates} updates')

    # Cleanup
    string = """
        drop table tmp_meta;
        drop table tmp_streams;
        drop table new_artists;
        drop table streams;
        drop table updates;
        drop table inserts;
    """
    db.execute(string)

def songsDbUpdates(db, meta, streams, date, country_name):

    # Create temporary tables
    string = """
        create temp table tmp_meta (
            unified_song_id text,
            title text,
            artist text,
            isrc text
        );

        create temp table tmp_streams (
            unified_song_id text,
            date date,
            streams int
        );
    """
    db.execute(string)

    # Fill temporary tables
    db.big_insert(meta, 'tmp_meta')
    db.big_insert(streams, 'tmp_streams')

    # Insert metadata & update isrcs
    string = """
        create temp table new_songs as (
            select
                tm.unified_song_id,
                tm.artist,
                tm.title,
                tm.isrc,
                true as is_global
            from tmp_meta tm
            left join nielsen_song.meta m on tm.unified_song_id = m.unified_song_id
            where m.unified_song_id is null
        );

        insert into nielsen_song.meta (unified_song_id, artist, title, isrc, is_global)
        select unified_song_id, artist, title, isrc, is_global from new_songs;

        select count(*) from new_songs;
    """
    new_songs = db.execute(string)
    num_new_songs = new_songs.loc[0, 'count']
    print(f'Inserted {num_new_songs} new songs')

    # Streams updates
    string = f"""
        create temp table streams as (
            select
                m.id as song_id,
                ts.date,
                ts.streams,
                case
                    when existing_streams.song_id is null then false
                    else true
                end as record_exists,
                existing_streams.{country_name} as existing_streams
            from tmp_streams ts
            left join nielsen_song.meta m on ts.unified_song_id = m.unified_song_id
            left join nielsen_song.streams existing_streams on m.id = existing_streams.song_id and ts.date = existing_streams.date
        );

        create temp table updates as (
            select song_id, date, streams
            from streams
            where record_exists is true
                and streams is distinct from existing_streams
        );

        create temp table inserts as (
            select song_id, date, streams
            from streams
            where record_exists is false
        );

        -- Update existing records
        update nielsen_song.streams s
        set {country_name} = updates.streams
        from updates
        where s.song_id = updates.song_id
            and s.date::date = updates.date::date;

        -- Insert new records
        insert into nielsen_song.streams (song_id, date, {country_name})
        select song_id, date, streams from inserts;

        select count(*) as value, 'updates' as name from updates
        union all
        select count(*) as value, 'inserts' as name from inserts;
    """
    params = { 'date': date }
    results = db.execute(string, params)

    num_inserts = results.loc[results['name'] == 'inserts', 'value'].iloc[0]
    num_updates = results.loc[results['name'] == 'updates', 'value'].iloc[0]
    print(f'{num_inserts} inserts | {num_updates} updates')

    # Cleanup
    string = """
        drop table tmp_meta;
        drop table tmp_streams;
        drop table new_songs;
        drop table streams;
        drop table updates;
        drop table inserts;
    """
    db.execute(string)

def cleanSongs(df, date):

    # Drop unnecessary columns
    song_drop_columns = [
        'Country Code',
        'Country',
        'WeekID',
        'Date',
        'Rank',
        'UnifiedArtistID',
        'Digital Song Sales - Current Day',
        'Digital Song Sales - % Change Prior Day',
        'Digital Song Sales - Prior Day',
        'Digital Song Sales - YTD',
        'Digital Song Sales - ATD 2022',
        'Streaming On-Demand Total - % Change Prior Day',
        'Streaming On-Demand Total - YTD',
        'Streaming On-Demand Total - ATD 2022'
    ]

    df = df.drop(columns=song_drop_columns)

    # Standardize columns
    current_day = datetime.strftime(date, '%Y-%m-%d')
    previous_day = datetime.strftime(date - timedelta(1), '%Y-%m-%d')

    song_rename_columns = {
        'UnifiedSongID': 'unified_song_id',
        'Title': 'title',
        'Artist': 'artist',
        'Top ISRC': 'isrc',
        'Streaming On-Demand Total - Current Day': current_day,
        'Streaming On-Demand Total - Prior Day': previous_day
    }

    df = df.rename(columns=song_rename_columns)

    # Separate into meta and streams
    meta_columns = [ 'unified_song_id', 'title', 'artist', 'isrc' ]
    streams_columns = [ 'unified_song_id', current_day, previous_day ]

    meta = df[meta_columns].drop_duplicates(subset=['unified_song_id'])
    streams = df[streams_columns].melt(id_vars='unified_song_id', var_name='date', value_name='streams')

    return meta, streams

def global_pipeline(settings):

    # Init root directory
    if os.path.exists(GLOBAL_ARCHIVE_FOLDER) == False:
        os.mkdir(GLOBAL_ARCHIVE_FOLDER)

    # Init today's directory
    daily_dir = GLOBAL_ARCHIVE_DAILY_FOLDER_TEMPLATE.format(datetime.strftime(settings['global_date'], '%Y-%m-%d'))
    if os.path.exists(daily_dir) == False:
        os.mkdir(daily_dir)

    total_time = Time()

    # Init db
    db = Db('rca_db')
    db.connect()

    # Loop through each server
    for server_name in GLOBAL_SERVER_NAMES:

        # Init the directory for this server
        server_dir = os.path.join(daily_dir, server_name)
        if os.path.exists(server_dir) == False:
            os.mkdir(server_dir)

        server_time = Time()

        # Init sftp client for this connection
        sftp = Sftp(server_name)

        with sftp.connect() as sftp_conn:

            # Get the list of available files in the server which we use to check if the data is there or not
            filenames = sftp_conn.listdir(NIELSEN_GLOBAL_FILES_LOCATION)

            # We're going to loop through each country on the server
            for country_name in GLOBAL_COUNTRY_NAMES[server_name]:

                print(country_name)

                country_time = Time()

                # Create the file names that pertain to the current country
                formatted_date = datetime.strftime(settings['global_date'], '%Y%m%d')
                artist_filename = ARTIST_NAME_TEMPLATE.format(country_name, formatted_date)
                song_filename = SONG_NAME_TEMPLATE.format(country_name, formatted_date)

                # Create the fullfiles
                artist_remote_fullfile = os.path.join(NIELSEN_GLOBAL_FILES_LOCATION, artist_filename)
                artist_local_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, artist_filename)

                song_remote_fullfile = os.path.join(NIELSEN_GLOBAL_FILES_LOCATION, song_filename)
                song_local_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, song_filename)

                # ARTISTS

                # If the filename doesn't exist in the server list, skip it
                if artist_filename in filenames:

                    # Download files from the server if they don't exist already
                    if os.path.exists(artist_local_fullfile) is False:
                        sftp_conn.get(artist_remote_fullfile, artist_local_fullfile)

                    try:

                        # Read, clean and update
                        df = pd.read_csv(artist_local_fullfile, delimiter='\t', encoding='UTF-16')

                        meta, streams = cleanArtists(df, settings['global_date'])
                        artistsDbUpdates(db, meta, streams, settings['global_date'], country_name.lower())

                    except EmptyDataError:
                        print(artist_local_fullfile + ' is empty!')

                    # Move file to archive
                    artist_archive_fullfile = os.path.join(server_dir, os.path.basename(artist_local_fullfile))
                    os.rename(artist_local_fullfile, artist_archive_fullfile)

                else:

                    print(f'{artist_filename} not in {server_name} server...skipping...')

                # SONGS

                # If the filename doesn't exist in the server list, skip it
                if song_filename in filenames:

                    # Download files from the server if they don't exist already
                    if os.path.exists(song_local_fullfile) is False:
                        sftp_conn.get(song_remote_fullfile, song_local_fullfile)

                    try:

                        # Read, clean and update
                        df = pd.read_csv(song_local_fullfile, delimiter='\t', encoding='UTF-16')

                        meta, streams = cleanSongs(df, settings['global_date'])
                        songsDbUpdates(db, meta, streams, settings['global_date'], country_name.lower())

                    except EmptyDataError:
                        print(song_local_fullfile + ' is empty!')

                    # Move file to archive
                    song_archive_fullfile = os.path.join(server_dir, os.path.basename(song_local_fullfile))
                    os.rename(song_local_fullfile, song_archive_fullfile)

                else:

                    print(f'{song_filename} not in {server_name} server...skipping...')
                
                print(chalk.cyan(f'{country_name} processed | {country_time.getElapsed()}'))

        print(chalk.red(f'{server_name} complete | {server_time.getElapsed()}'))

    # Make archive
    print('Making archive...')
    zip_dir = GLOBAL_ARCHIVE_DAILY_ZIP_FOLDER_TEMPLATE.format(datetime.strftime(settings['global_date'], '%Y-%m-%d'))
    make_archive(daily_dir, zip_dir)

    # Remove the non-zip folder
    shutil.rmtree(daily_dir)

    if settings['is_testing'] == True:
        db.rollback()
    else:
        db.commit()

    print(chalk.green(f'Complete! Total time: {total_time.getElapsed()}'))