# !important This pipeline is still in beta, as we have not committed to our global partnership with Nielsen yet

from .env import LOCAL_DOWNLOAD_FOLDER
from datetime import datetime, timedelta
from .Sftp import Sftp
from .PipelineBase import PipelineBase
from pandas.errors import EmptyDataError
import pandas as pd
import random
import os

# Vars
NIELSEN_GLOBAL_FILES_LOCATION = '/' # Location on the remote global servers where the files exist
GLOBAL_ARCHIVE_FOLDER = './global_archive'
GLOBAL_ARCHIVE_DAILY_FOLDER_TEMPLATE = os.path.join(GLOBAL_ARCHIVE_FOLDER, 'global_{}')
GLOBAL_ARCHIVE_DAILY_ZIP_FOLDER_TEMPLATE = os.path.join(GLOBAL_ARCHIVE_FOLDER, 'global_{}')
GLOBAL_S3_UPLOAD_FOLDER_TEMPLATE = 'nielsen_archive/global/{}'

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

SONGS_STR_INDICATOR = '_Daily_Top50k_Songs_'
ARTISTS_STR_INDICATOR = '_Daily_Top20k_Artists_'

class NielsenDailyGlobalPipeline(PipelineBase):

    def __init__(self, db_name):
        PipelineBase.__init__(self, db_name)
        self.sftp_conn_name = None
        self.sftp_conn = None

    def getNewFiles(self):

        """
            Constructs a list of all the new files that we need to process
            during this session.
        """
        server_files = []
        for server_name in GLOBAL_SERVER_NAMES:
            new_files = self.getNewFilesFromServer(server_name)
            server_files.append(new_files)

        return pd.concat(server_files)

    def getTestFiles(self):

        """
            Get 1 random artist and 1 random song file to do test processing on.
            Select server randomly as well.
        """
        server_name = random.choice(GLOBAL_SERVER_NAMES)
        sftp = Sftp(server_name)
        filenames = sftp.list()


        files = []
        song_filename = [i for i in filenames if SONGS_STR_INDICATOR in i and '__NO_DATA' not in i][0]
        artist_filename = [i for i in filenames if ARTISTS_STR_INDICATOR in i and '__NO_DATA' not in i][0]

        # Deconstruct the filename for the different parts
        tmp_song_filename = song_filename.replace('.tsv', '') # remove extension
        country, date = tmp_song_filename.split(SONGS_STR_INDICATOR) # now in between the indicator you can get the country & date
        date = datetime.strptime(date, '%Y%m%d') # convert the date string to a date object

        files.append({
            'filename': song_filename,
            'date': date,
            'country': country.lower(),
            'server_name': server_name,
            'type': 'songs'
        })

        # Deconstruct the filename for the different parts
        tmp_artist_filename = artist_filename.replace('.tsv', '') # remove extension
        country, date = tmp_artist_filename.split(ARTISTS_STR_INDICATOR) # now in between the indicator you can get the country & date
        date = datetime.strptime(date, '%Y%m%d') # convert the date string to a date object

        files.append({
            'filename': artist_filename,
            'date': date,
            'country': country.lower(),
            'server_name': server_name,
            'type': 'artists'
        })

        files_df = pd.DataFrame(files)

        if len(files_df) != 2:
            raise Exception('Unable to find 2 valid test files')

        return files_df

    def getNewFilesFromServer(self, server_name):

        """
            Creates two dataframes, each represent new files on the server_name that
            need to be processed in this session. One dataframe is for artists, the other for songs.
        """

        # Get all the existing files on the server
        sftp = Sftp(server_name)
        filenames = sftp.list()

        # First construct a list of all the relevant filenames
        # We only care about the song & artist files (not isrc files)
        # Remove files with __NO_DATA in the filename
        # Construct into an object with relevant info for easy handling as well
        files = []
        for filename in filenames:
            
            # If true then we're looking at a song file
            if SONGS_STR_INDICATOR in filename and '__NO_DATA' not in filename:

                # Deconstruct the filename for the different parts
                tmp_filename = filename.replace('.tsv', '') # remove extension
                country, date = tmp_filename.split(SONGS_STR_INDICATOR) # now in between the indicator you can get the country & date
                date = datetime.strptime(date, '%Y%m%d') # convert the date string to a date object
                    
                files.append({
                    'filename': filename,
                    'date': date,
                    'country': country.lower(),
                    'server_name': server_name,
                    'type': 'songs'
                })

            # If true then we're looking at an artist file
            if ARTISTS_STR_INDICATOR in filename and '__NO_DATA' not in filename:

                # Deconstruct the filename for the different parts
                tmp_filename = filename.replace('.tsv', '') # remove extension
                country, date = tmp_filename.split(ARTISTS_STR_INDICATOR) # now in between the indicator you can get the country & date
                date = datetime.strptime(date, '%Y%m%d') # convert the date string to a date object

                files.append({
                    'filename': filename,
                    'date': date,
                    'country': country.lower(),
                    'server_name': server_name,
                    'type': 'artists'
                })

        files_df = pd.DataFrame(files)

        # Next we need to compare these files to the files we've already processed to see which ones we need to process
        string = """
            create temp table tmp_global_files (
                filename text,
                date date,
                country text,
                server_name text,
                type text
            );
        """
        self.db.execute(string)
        self.db.big_insert(files_df, 'tmp_global_files')

        string = """
            select t.*
            from tmp_global_files t
            left join misc.nielsen_global_daily_files_completed e on t.filename = e.filename
            where e.file_id is null
        """
        files_df = self.db.execute(string)

        string = """
            drop table tmp_global_files;
        """
        self.db.execute(string)

        # If None we did something wrong
        if files_df is None:
            raise Exception('Error while parsing artists/songs files')

        return files_df

    def processArtists(self, file):

        def clean(df, date):

            """
                Clean the global daily artist file.
            """

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

        def dbUpdates(meta, streams, date, country_name):

            """
                Perform database updates on a cleaned global daily artist file.
            """

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
            self.db.execute(string)

            # Fill temporary tables
            self.db.big_insert(meta, 'tmp_meta')
            self.db.big_insert(streams, 'tmp_streams')

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
            new_artists = self.db.execute(string)
            if new_artists is None:
                raise Exception('Error inserting new artists')
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
            results = self.db.execute(string, params)

            if results is None:
                raise Exception('Error updating artist streams')
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
            self.db.execute(string)

        df, fullfiles = self.initFileProcess(file)

        meta, streams = clean(df, file['date'])
        dbUpdates(meta, streams, file['date'], file['country'])

        self.finishFileProcess(file, fullfiles)

    def processSongs(self, file):
        
        def clean(df, date):

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

        def dbUpdates(meta, streams, date, country_name):

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
            self.db.execute(string)

            # Fill temporary tables
            self.db.big_insert(meta, 'tmp_meta')
            self.db.big_insert(streams, 'tmp_streams')

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
            new_songs = self.db.execute(string)
            if new_songs is None:
                raise Exception('Error inserting new songs')
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
            results = self.db.execute(string, params)

            if results is None:
                raise Exception('Error updating song streams')
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
            self.db.execute(string)

        df, fullfiles = self.initFileProcess(file)

        meta, streams = clean(df, file['date'])
        dbUpdates(meta, streams, file['date'], file['country'])

        self.finishFileProcess(file, fullfiles)

    def initFileProcess(self, file):

        # Make sure that we are set to the right connection
        if self.sftp_conn is None or self.sftp_conn_name is None or file['server_name'] != self.sftp_conn_name:
            sftp = Sftp(file['server_name'])
            self.sftp_conn = sftp.connect()
            self.sftp_conn_name = file['server_name']

        # Create the fullfiles
        remote_fullfile = os.path.join(NIELSEN_GLOBAL_FILES_LOCATION, file['filename'])
        local_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, file['filename'])
        s3_fullfile = GLOBAL_S3_UPLOAD_FOLDER_TEMPLATE.format(file['filename'])

        # Download files from the server if they don't exist already
        if os.path.exists(local_fullfile) is False:
            self.sftp_conn.get(remote_fullfile, local_fullfile)

        try:

            # Read, clean and update
            df = pd.read_csv(local_fullfile, delimiter='\t', encoding='UTF-16')

            # Subset if we're testing
            if self.settings['is_testing'] == True:
                df = df.iloc[:100].reset_index(drop=True)

            return df, {
                'remote_fullfile': remote_fullfile,
                'local_fullfile': local_fullfile,
                's3_fullfile': s3_fullfile
            }

        except EmptyDataError:
            print(local_fullfile + ' is empty!')
            raise Exception('Empty file: ' + file['filename'])

    def finishFileProcess(self, file, fullfiles):

        # We don't wanna do any of this unless we actually processed the the file
        if self.settings['is_testing'] == False:

            # Archive
            self.aws.upload_s3(fullfiles['local_fullfile'], fullfiles['s3_fullfiles'])

            # Mark that we've processed this file
            string = """
                insert into misc.nielsen_global_daily_files_completed (filename, date, country, server_name, type)
                values (%(filename)s, %(date)s, %(country)s, %(server_name)s, %(type)s)
            """
            self.db.execute(string, file)

            # Normally a pipeline would commit at the end, but we're going to commit after each file process
            self.commit()

        # Must do this after uploading to s3
        os.remove(fullfiles['local_fullfile'])

    def addProcessFuncFromFile(self, file):

        processFunc = None
        if file['type'] == 'artists':
            processFunc = lambda: self.processArtists(file)
        elif file['type'] == 'songs':
            processFunc = lambda: self.processSongs(file)

        if processFunc is None:
            raise Exception('Not a valid process func')

        self.add_function(processFunc, f"Global file: {file['filename']}")

    def build(self):

        print('Building function structure...')
        
        # Get all the files from the server that are available to be processed
        files = self.getNewFiles()

        # Sort them by date & server name
        # date, because we need to process them in the correct order (oldest->newest)
        # server name, so that we don't have to change sftp connections every time we change files
        files = files.sort_values(by=['server_name', 'date'], ascending=True).reset_index(drop=True).to_dict('records')

        # Create a process function for each file separately
        for file in files:
            self.addProcessFuncFromFile(file)

    def test_build(self):

        print('Building function structure...')
        
        files = self.getTestFiles()
        files = files.to_dict('records')
        for file in files:
            self.addProcessFuncFromFile(file)