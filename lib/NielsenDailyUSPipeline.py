from .env import LOCAL_ARCHIVE_FOLDER, LOCAL_DOWNLOAD_FOLDER, REPORTS_FOLDER
from .PipelineBase import PipelineBase
from .functions import chunker, getSpotifyTrackDataFromSpotifyUsingIsrcTitleAndArtist
from .Sftp import Sftp
from .BinnedModel import BinnedModel
from .Spotify import Spotify
from .Fuzz import Fuzz
from .Db import Db
from .RapidApi import RapidApi
from .functions import today
from datetime import datetime, timedelta
from zipfile import ZipFile
import requests
import unicodedata
from uuid import uuid4
import pandas as pd
import random
import os

NIELSEN_US_DAILY_ARCHIVE_FOLDER = '/' # location on nielsen's remote sftp server where the US daily files are located
NIELSEN_US_DAILY_ZIP_FILENAME = 'RCA_AR_Daily_Report_{}.zip'
NIELSEN_US_DAILY_ARTIST_FILENAME = 'RCA_AR_Artist_ODA_{}.csv'
NIELSEN_US_DAILY_SONG_FILENAME_OLD = 'RCA_AR_SONG_ODA_{}.csv'
NIELSEN_US_DAILY_SONG_FILENAME = 'RCA_AR_SONG_ODA_{}_New.csv'
EXPORTS_TEMPLATE = '{}_exports'
US_S3_UPLOAD_FOLDER_TEMPLATE = 'nielsen_archive/us/{}'
MAC_FOLDER = '__MACOSX'

def str2Date(s):
    """Convert a string to a date object"""
    try:
        return datetime.strptime(s, '%m/%d/%Y')
    except ValueError:
        try:
            return datetime.strptime(s, '%Y-%m-%d')
        except ValueError:
            return False

def getDateCols(cols):
    """Get date columns from nielsen file columns"""
    idx = []
    for col in cols:
        d = str2Date(col)
        if d:
            idx.append(col)
    return idx

def getDateIndicies(cols):
    """Convert columns into correct date indicies"""
    idx = []
    for col in cols:
        d = str2Date(col)
        if d:
            idx.append(col)
    return idx

def tDate(d):
    """Convert to property datetime format for merging later"""
    m, d, y = d.split('/')
    return f'{y}-{m}-{d}'

def transformSpotifyArtistObject(artist):
    """Parse artist response object from spotify"""

    def get_image(arr):

        if len(arr) == 0:
            return None
        else:
            arr.sort(key=lambda x: x['height'], reverse=True)
            return arr[0]['url']

    url = artist['external_urls']['spotify'] if 'external_urls' in artist and 'spotify' in artist['external_urls'] else None
    followers = artist['followers']['total'] if 'followers' in artist and 'total' in artist['followers'] else None
    genres = '/'.join(artist['genres']) if artist['genres'] is not None else None
    api_url = artist['href'] if 'href' in artist else None
    spotify_artist_id = artist['id'] if 'id' in artist else None
    spotify_image = get_image(artist['images'])
    name = artist['name'] if 'name' in artist else None
    popularity = artist['popularity'] if 'popularity' in artist else None
    uri = artist['uri'] if 'uri' in artist else None

    return {
        'url': url,
        'followers': followers,
        'genres': genres,
        'api_url': api_url,
        'spotify_artist_id': spotify_artist_id,
        'spotify_image': spotify_image,
        'name': name,
        'popularity': popularity,
        'uri': uri
    }

class NielsenDailyUSPipeline(PipelineBase):

    def __init__(self, db_name):
        PipelineBase.__init__(self, db_name)

        # Make folders if they don't already exist
        if os.path.isdir(LOCAL_ARCHIVE_FOLDER) == False:
            os.mkdir(LOCAL_ARCHIVE_FOLDER)

        if os.path.isdir(LOCAL_DOWNLOAD_FOLDER) == False:
            os.mkdir(LOCAL_DOWNLOAD_FOLDER)

        if os.path.isdir(REPORTS_FOLDER) == False:
            os.mkdir(REPORTS_FOLDER)

        # Format date to fit the report name
        formatted_date = datetime.strftime(self.settings['date'], '%Y%m%d')

        # Filenames
        zip_filename = NIELSEN_US_DAILY_ZIP_FILENAME.format(formatted_date)
        old_song_filename = NIELSEN_US_DAILY_SONG_FILENAME_OLD.format(formatted_date)
        song_filename = NIELSEN_US_DAILY_SONG_FILENAME.format(formatted_date)
        artist_filename = NIELSEN_US_DAILY_ARTIST_FILENAME.format(formatted_date)

        # Fullfiles
        zip_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, zip_filename)
        zip_remote_archive_fullfile = os.path.join(NIELSEN_US_DAILY_ARCHIVE_FOLDER, zip_filename)
        zip_local_archive_fullfile = os.path.join(LOCAL_ARCHIVE_FOLDER, zip_filename)
        old_song_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, NIELSEN_US_DAILY_SONG_FILENAME_OLD.format(formatted_date))
        song_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, NIELSEN_US_DAILY_SONG_FILENAME.format(formatted_date))
        artist_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, NIELSEN_US_DAILY_ARTIST_FILENAME.format(formatted_date))

        self.files = {
            'zip': zip_filename,
            'old_song': old_song_filename,
            'song': song_filename,
            'artist': artist_filename
        }

        self.fullfiles = {
            'zip': zip_fullfile,
            'zip_local_archive': zip_local_archive_fullfile,
            'zip_remote_archive': zip_remote_archive_fullfile,
            'old_song': old_song_fullfile,
            'song': song_fullfile,
            'artist': artist_fullfile
        }

        self.folders = {
            'exports': os.path.join(REPORTS_FOLDER, EXPORTS_TEMPLATE.format(formatted_date))
        }

        self.reports_fullfiles = {
            'genius_scrape': os.path.join(self.folders['exports'], f'genius_scrape_{formatted_date}.csv'),
            'nielsen_daily_audio': os.path.join(self.folders['exports'], f'nielsen_daily_songs_{formatted_date}.csv'),
            'shazam_by_market_streams': os.path.join(self.folders['exports'], f'shazam_by_market_streams_{formatted_date}.csv'),
            'shazam_by_market': os.path.join(self.folders['exports'], f'shazam_by_market_{formatted_date}.csv'),
            'shazam_rank_by_country': os.path.join(self.folders['exports'], f'shazam_rank_by_country_{formatted_date}.csv'),
            'spotify_artist_stat_growth': os.path.join(self.folders['exports'], f'spotify_artist_stat_growth_{formatted_date}.csv'),
            'artist_8_week_growth': os.path.join(self.folders['exports'], f'artist_8_week_growth_{formatted_date}.csv'),
            'song_8_week_growth': os.path.join(self.folders['exports'], f'song_8_week_growth_{formatted_date}.csv'),
            'growing_genres': os.path.join(self.folders['exports'], f'growing_genres_{formatted_date}.csv'),
            'nielsen_weekly_audio': os.path.join(self.folders['exports'], f'nielsen_weekly_audio_{formatted_date}.csv'),
            'new_artists_in_genres_tw_streams': os.path.join(self.folders['exports'], f'new_artists_in_genres_tw_streams_{formatted_date}.csv'),
            'sp_follower_growth': os.path.join(self.folders['exports'], f'sp_follower_growth_{formatted_date}.csv'),
            'ig_follower_growth': os.path.join(self.folders['exports'], f'ig_follower_growth_{formatted_date}.csv'),
            'tt_follower_growth': os.path.join(self.folders['exports'], f'tt_follower_growth_{formatted_date}.csv')
        }

    def downloadFiles(self):

        """
            If we haven't already downloaded the files, download them
            from nielsen.
        """

        # Only download if some file is missing
        if os.path.exists(self.fullfiles['artist']) == False or os.path.exists(self.fullfiles['song']) == False:

            # Init SFTP client
            sftp = Sftp('nielsen_daily')

            # Download remote zip file to our local download folder
            sftp.get(self.fullfiles['zip_remote_archive'], self.fullfiles['zip'])

            # Unzip
            with ZipFile(self.fullfiles['zip'], 'r') as file_ref:
                file_ref.extractall(LOCAL_DOWNLOAD_FOLDER)

            # We can delete the old song file
            os.remove(self.fullfiles['old_song'])

            # Move the zip files into the archive (don't do this unless we have the required files)
            os.rename(self.fullfiles['zip'], self.fullfiles['zip_local_archive'])

            # Remove this annoying folder that sometimes comes from our zip files
            if os.path.isdir(os.path.join(LOCAL_DOWNLOAD_FOLDER, MAC_FOLDER)):
                os.rmdir(MAC_FOLDER)

            # Create an exports directory
            if os.path.isdir(self.folders['exports']) == False:
                os.mkdir(self.folders['exports'])

            print(f"Initialized file: {self.fullfiles['zip']}")

        else:

            print(f"File {self.fullfiles['zip']} already initialized")

    def deleteFiles(self):

        """
            Delete the files we've downloaded from nielsen. This isn't a big deal
            if we're running it from docker because the container will be destroyed anyway,
            but just in case we're running it locally on our computer, we'll do this anyway.
        """

        # Delete artist file
        if os.path.exists(self.fullfiles['artist']):
            os.remove(self.fullfiles['artist'])

        # Delete song file
        if os.path.exists(self.fullfiles['song']):
            os.remove(self.fullfiles['song'])

    def validateSession(self):

        """
            In order to reduce the chances of making an error deep down the line in our pipeline,
            which would be really annoying because then we just wasted a lot of time running everything
            leading up to that error, we should validate that everything we expect to work, should work.

            - File's we expect to be there, should be there
            - Those file formats should be reasonable (i.e. let's not run a whole pipeline on useless data we're going to need to delete and replace)
            - We should be able to connect to and select data from the databases that we use
            - Spotify's api should be responsive
        """

        # Our artist / song files should exist
        if os.path.exists(self.fullfiles['artist']) == False:
            raise Exception(f"Missing artist file from zip: {self.fullfiles['zip']}")

        if os.path.exists(self.fullfiles['song']) == False:
            raise Exception(f"Missing song file from zip: {self.fullfiles['zip']}")

        print('Check 1: Files exist')

        # We should be able to read those files into a dataframe
        artists = pd.read_csv(self.fullfiles['artist'], encoding='UTF-16')
        songs = pd.read_csv(self.fullfiles['song'], encoding='UTF-16')

        print('Check 2: Files are readable')

        # We should have all the columns that we expect
        def generateDateColumns(starting_date):

            # Generate daily streaming date columns
            date_columns = []
            for i in range(14):
                date_columns.append(datetime.strftime(starting_date - timedelta(i), '%m/%d/%Y'))

            return date_columns

        def hasRequiredColumns(df, required_columns, filename):

            for col in required_columns:
                if col not in df.columns:
                    raise Exception(f'{filename} file is missing required column: {col}')

        # Create columns for artist file
        artist_date_columns = generateDateColumns(self.settings['date'] - timedelta(days=2))
        artist_required_columns = [
            'TW Rank', 'LW Rank', 'Artist', 'UnifiedArtistID',
            'TW On-Demand Audio Streams', 'LW On-Demand Audio Streams',
            'L2W_On_Demand_Audio_Streams', 'Weekly %change On-Demand Audio Streams',
            'YTD On-Demand Audio Streams', 'RTD On-Demand Audio Streams',
            'WTD Building ODA (Friday-Thursday)', '7-day rolling ODA',
            'pre-7 day rolling oda', 'TW Album Sales', 'YTD Album Sales', 'RTD Album Sales',
            'TW Digital Track Sales', 'YTD Digital Track Sales',
            'RTD Digital Track Sales', 'TW On-Demand Video', 'LW On-Demand Video',
            'YTD On-Demand Video', 'RTD On-Demand Video',
            *artist_date_columns
        ]

        # Create columns for song file
        song_date_columns = generateDateColumns(self.settings['date'] - timedelta(days=2))
        song_date_columns_mod = []
        for date in song_date_columns:
            song_date_columns_mod.append(date + ' - Total ODA')
            song_date_columns_mod.append(date + ' - Premium ODA')
            song_date_columns_mod.append(date + ' - Ad Supported ODA')
        song_required_columns = [
            'TW Rank', 'LW Rank', 'Artist', 'Title', 'Unified Song Id',
            'Label Abbrev', 'CoreGenre', 'Release_date', 'Top ISRC',
            'TW On-Demand Audio Streams', 'LW On-Demand Audio Streams',
            'L2W_On_Demand_Audio_Streams', 'Weekly %change On-Demand Audio Streams',
            'YTD On-Demand Audio Streams', 'RTD On-Demand Audio Streams',
            'RTD On-Demand Audio Streams - Premium',
            'RTD On-Demand Audio Streams - Ad Supported',
            'WTD Building ODA (Friday-Thursday)', '7-day Rolling ODA',
            'pre-7days rolling ODA', 'TW Digital Track Sales',
            'YTD Digital Track Sales', 'ATD Digital Track Sales',
            'TW On-Demand Video', 'LW On-Demand Video', 'YTD On-Demand Video',
            'ATD On-Demand Video',
            *song_date_columns_mod
        ]

        # Validate that all the required columns exist
        hasRequiredColumns(artists, artist_required_columns, 'Artists')
        hasRequiredColumns(songs, song_required_columns, 'Songs')

        print('Check 3: Files are formatted properly')

        # We should be able to connect to both databases and make a query
        postgres_db = Db(self.db_name)
        postgres_db.test()

        print('Check 4: Postgres db valid')

        reporting_db = Db('reporting_db')
        reporting_db.test()

        print('Check 5: Reporting db valid')

        # Test Spotify API availability
        sp = Spotify()
        sp.test()

        print('Check 6: Spotify Api valid')
        
        print('All checks passed!')

    def findSignedByCopyrights(self, df):

        """

            Basic check between copyrights and our list of labels.

            Uses: misc.list_of_labels

            @param df(copyrights, signed, *)
            @returns df(copyrights, signed, *)

        """
        
        def findSignedByCopyrightsFilter(row, labels):
            
            if row['signed'] == True:
                return True

            # Extract values
            df_label = row['copyrights'].lower()

            # Find matches
            res = [ele for ele in labels if(ele.lower() in df_label)]
            res = bool(res)

            if res == True:
                return True
            else:
                return False

        # Load in nielsen_labels
        nielsen_labels = self.db.execute('select * from misc.list_of_labels')

        if nielsen_labels is None:
            raise Exception('Nielsen labels do not exist.')

        labels = nielsen_labels['label'].values

        # Fill na values to avoid errors
        df['copyrights'] = df['copyrights'].fillna('')

        # Apply filter fn
        df['signed'] = df.apply(findSignedByCopyrightsFilter, labels=labels, axis=1)

        return df
     
    def basicSignedCheck(self, df):

        """
            @param df('copyrights', 'signed', 'artist')
            @param | df with columns: copyrights(str) | signed(bool)
        """

        # Apply another layer of detecting 'signed' with a running list of signed artists
        def filterBySignedArtistsList(df):
            
            def filterBySignedArtistsListFilter(row, fuzz):

                # If we already know they're signed, return
                if row['signed'] == True:
                    return True

                # Preprocess the artist name
                artist = row['artist']

                # Check against fuzzyset
                ratio, _, _ = fuzz.check(artist)

                if ratio >= 0.95:
                    return True
                else:
                    return False

            # Read in the csv for signed_artists
            artists_df = self.db.execute('select * from misc.signed_artists')

            if artists_df is None:
                raise Exception('Missing signed artists template.')

            artists = artists_df.drop_duplicates(keep='first').artist.values

            # Create fuzzyset
            fuzz = Fuzz(artists)

            # Check each artist name against fuzzyset and determine if they are signed
            df['signed'] = df.apply(filterBySignedArtistsListFilter, fuzz=fuzz, axis=1)

            return df

        df = self.findSignedByCopyrights(df)

        if 'artist' in df:
            df = filterBySignedArtistsList(df)

        return df

    def cleanArtists(self, df):
        
        # Rename columns for consistency & database usage
        rename_columns = {
            'TW Rank': 'tw_rank',
            'LW Rank': 'lw_rank',
            'Artist': 'artist',
            'UnifiedArtistID': 'unified_artist_id',
            'TW On-Demand Audio Streams': 'tw_oda_streams',
            'LW On-Demand Audio Streams': 'lw_oda_streams',
            'L2W_On_Demand_Audio_Streams': 'l2w_oda_streams',
            'Weekly %change On-Demand Audio Streams': 'weekly_pct_chg_oda_streams',
            'YTD On-Demand Audio Streams': 'ytd_oda_streams',
            'RTD On-Demand Audio Streams': 'rtd_oda_streams',
            'WTD Building ODA (Friday-Thursday)': 'wtd_building_oda_fri_thurs',
            '7-day rolling ODA': 'tw_rolling_oda',
            'pre-7 day rolling oda': 'lw_rolling_oda',
            'TW Album Sales': 'tw_album_sales',
            'YTD Album Sales': 'ytd_album_sales',
            'RTD Album Sales': 'rtd_album_sales',
            'TW Digital Track Sales': 'tw_digital_track_sales',
            'YTD Digital Track Sales': 'ytd_digital_track_sales',
            'RTD Digital Track Sales': 'rtd_digital_track_sales',
            'TW On-Demand Video': 'tw_odv',
            'LW On-Demand Video': 'lw_odv',
            'YTD On-Demand Video': 'ytd_odv',
            'RTD On-Demand Video': 'rtd_odv'
        }

        df.rename(columns=rename_columns, inplace=True)

        # Drop unnecessary columns
        drop_columns = [
            'lw_rank',
            'lw_oda_streams',
            'l2w_oda_streams',
            'weekly_pct_chg_oda_streams',
            'ytd_oda_streams',
            'wtd_building_oda_fri_thurs',
            'tw_rolling_oda',
            'lw_rolling_oda',
            'ytd_album_sales',
            'ytd_digital_track_sales',
            'lw_odv',
            'ytd_odv'
        ]

        df.drop(columns=drop_columns, errors='ignore', inplace=True)

        # Add signed column
        df['signed'] = False

        # Add a report_id column
        report_id = uuid4()
        df['report_id'] = report_id

        # Drop rows with null unified_artist_id
        df = df[~df['unified_artist_id'].isnull()].reset_index(drop=True)

        # Sometimes we have unified_artist_id duplicates from nielsen, ignore these.
        df = df.drop_duplicates(subset='unified_artist_id').reset_index(drop=True)

        # Clean types
        df = df.astype({
            'tw_rank': 'int',
            'artist': 'str',
            'unified_artist_id': 'int',
            'tw_oda_streams': 'int',
            'rtd_oda_streams': 'int',
            'tw_album_sales': 'int',
            'rtd_album_sales': 'int',
            'tw_digital_track_sales': 'int',
            'rtd_digital_track_sales': 'int',
            'tw_odv': 'int',
            'rtd_odv': 'int',
            'signed': 'bool'
        }).astype({ 'unified_artist_id': 'str' })

        # Extract only the meta columns
        meta_columns = [
            'tw_rank',
            'artist',
            'unified_artist_id',
            'tw_oda_streams',
            'rtd_oda_streams',
            'tw_album_sales',
            'rtd_album_sales',
            'tw_digital_track_sales',
            'rtd_digital_track_sales',
            'tw_odv',
            'rtd_odv',
            'signed',
            'report_id'
        ]

        meta = df[meta_columns].reset_index(drop=True)

        # Remove the meta information from the dataframe
        streams = df.drop(columns=meta_columns).reset_index(drop=True)

        # Reformat dates for streaming info
        streams_columns = { col: tDate(col) for col in streams.columns }
        streams.rename(columns=streams_columns, inplace=True)

        # Add the unified_artist_id back onto the streaming dataframes
        streams['unified_artist_id'] = meta['unified_artist_id']
        
        # Get the date indicies for the daily streaming data
        dateCols = getDateCols(streams)
        
        # Add a column to indicate the report date
        meta['report_date'] = pd.to_datetime(dateCols[0])

        # Pivot all the streaming data from wide to long format
        streams = streams.melt(id_vars='unified_artist_id', var_name='date', value_name='streams')

        # Make sure we don't have any null streaming values
        streams['streams']  = streams['streams'].fillna(0).astype('int')
        
        return meta, streams

    # Apply another layer of detecting 'signed' with a running list of signed artists
    def filterBySignedArtistsList(self, df):
        
        # Read in the signed artists that are tracked
        artists_df = self.db.execute('select * from misc.signed_artists')

        if artists_df is None:
            raise Exception('Missing signed artists template')

        artists = artists_df['artist'].values
        
        # Check if they're in our signed list
        df.loc[df['artist'].isin(artists), 'signed'] = True
        
        return df

    def prepareArtistData(self, df):
        
        # Clean & standardize data
        meta, streams = self.cleanArtists(df)
        
        # Add the 'signed' status to the artist
        meta = self.filterBySignedArtistsList(meta)
        
        return meta, streams

    def artistsDbUpdates(self, meta, streams):

        # META
        string = """
            create temp table tmp_meta (
                tw_rank int,
                artist text,
                unified_artist_id text,
                tw_oda_streams int,
                rtd_oda_streams bigint,
                tw_album_sales int,
                rtd_album_sales int,
                tw_digital_track_sales int,
                rtd_digital_track_sales bigint,
                tw_odv int,
                rtd_odv bigint,
                signed boolean,
                report_id uuid,
                report_date date
            );
        """
        self.db.execute(string)
        self.db.big_insert(meta, 'tmp_meta')

        # REPORTS
        string = """
            -- Insert new artists into meta table
            insert into nielsen_artist.meta (artist, unified_artist_id)
            select artist, unified_artist_id from tmp_meta
            on conflict (unified_artist_id) do update
            set
                artist = excluded.artist,
                is_global = false;

            -- Insert reports
            insert into nielsen_artist.reports (
                artist_id, report_id, tw_rank, tw_oda_streams, rtd_oda_streams, tw_album_sales, rtd_album_sales,
                tw_digital_track_sales, rtd_digital_track_sales, tw_odv, rtd_odv, signed, report_date
            )
            select
                m.id as artist_id,
                tm.report_id,
                tm.tw_rank,
                tm.tw_oda_streams,
                tm.rtd_oda_streams,
                tm.tw_album_sales,
                tm.rtd_album_sales,
                tm.tw_digital_track_sales,
                tm.rtd_digital_track_sales,
                tm.tw_odv,
                tm.rtd_odv,
                tm.signed,
                tm.report_date
            from tmp_meta tm
            left join nielsen_artist.meta m on tm.unified_artist_id = m.unified_artist_id;
        """
        self.db.execute(string)

        # STREAMS
        string = """
            create temp table tmp_streams (
                unified_artist_id text,
                date date,
                streams int
            );
        """
        self.db.execute(string)
        self.db.big_insert(streams, 'tmp_streams')
        
        # Streaming inserts / updates
        string = """
            create temp table streams as (
                select
                    m.id as artist_id,
                    ts.date,
                    ts.streams,
                    case
                        when existing_streams.artist_id is null then false
                        else true
                    end as record_exists,
                    existing_streams.streams as existing_streams
                from tmp_streams ts
                left join nielsen_artist.meta m on ts.unified_artist_id = m.unified_artist_id
                left join (
                    select s.*
                    from nielsen_artist.streams s
                    where date > now() - interval '20 days'
                        and artist_id in (
                            select m.id
                            from tmp_meta tm
                            left join nielsen_artist.meta m on tm.unified_artist_id = m.unified_artist_id
                        )
                ) existing_streams on m.id = existing_streams.artist_id and ts.date = existing_streams.date
            );

            create temp table updates as (
                select
                    artist_id,
                    date,
                    streams
                from streams s
                where record_exists is true
                    and streams is distinct from existing_streams
            );

            create temp table inserts as (
                select
                    artist_id,
                    date,
                    streams
                from streams
                where record_exists is false
            );

            update nielsen_artist.streams s
            set streams = updates.streams
            from updates
            where s.artist_id = updates.artist_id
            and s.date::date = updates.date::date;

            insert into nielsen_artist.streams (artist_id, date, streams)
            select artist_id, date::date, streams from inserts;

            select count(*) as value, 'updates' as name from updates
            union all
            select count(*) as value, 'inserts' as name from inserts
        """
        results = self.db.execute(string)

        # Clean up
        string = """
            drop table tmp_meta;
            drop table tmp_streams;
            drop table streams;
            drop table inserts;
            drop table updates;
        """
        self.db.execute(string)

        if results is None:
            raise Exception('Error getting artist updates')

        # RESULTS
        num_inserts = results.loc[results['name'] == 'inserts', 'value'].iloc[0]
        num_updates = results.loc[results['name'] == 'updates', 'value'].iloc[0]
        print(f'Artist updates: {num_inserts} inserts | {num_updates} updates')

    def processArtists(self):

        """
            Official method for processing nielsen's Artist file.
        """

        # Read in the data
        df = pd.read_csv(self.fullfiles['artist'], encoding='UTF-16')

        # If we're in test mode, just take a subset
        if self.settings['is_testing'] == True:
            df = df.iloc[:100].reset_index(drop=True)

        # Clean dataframe
        meta, streams = self.prepareArtistData(df)

        # Database updates
        self.artistsDbUpdates(meta, streams)

    def filterSignedSongs(self, df):
        
        def filterSignedFilter(row, labels, labels_fuzz, artists):
        
            # Extract values
            df_label = row['label'].lower()

            # Find matches
            res = [ele for ele in labels if(ele.lower() in df_label)]
            res = bool(res)

            # If we found the label match then return it
            if res:
                return True
            
            # If we didn't find the label match, then our fallback is to fuzzy match
            ratio, match, _ = labels_fuzz.check(df_label)

            # If the fuzzy ratio is over 90% then you can say we found a match
            if df_label == match or ratio > 0.9:
                return True

            # Last chance, if it's in our list of signed artists
            if row['artist'] in artists:
                return True
            
            # Passed no checks, mark unsigned
            return False
        
        # Load in nielsen_labels
        nielsen_labels = self.db.execute('select * from misc.nielsen_labels')

        if nielsen_labels is None:
            raise Exception('Error getting nielsen labels template')

        labels = nielsen_labels['label'].values
        
        # Create Fuzz
        labels_fuzz = Fuzz(labels)
        
        # Read in the csv for signed_artists
        artists_df = self.db.execute('select * from misc.signed_artists where artist is not null')

        if artists_df is None:
            raise Exception('Error getting signed artists template')

        artists = artists_df.drop_duplicates(keep='first').artist.values
        
        df['signed'] = df.apply(filterSignedFilter, labels=labels, labels_fuzz=labels_fuzz, artists=artists, axis=1)
        
        return df

    def cleanSongs(self, df):
        
        # Rename the remaining columns for consistency and database usage
        renameable = {
            'TW Rank': 'tw_rank',
            'LW Rank': 'lw_rank',
            'Artist': 'artist',
            'Title': 'title',
            'Unified Song Id': 'unified_song_id',
            'Label Abbrev': 'label',
            'CoreGenre': 'core_genre',
            'Top ISRC': 'isrc',
            'Release_date': 'release_date',
            'TW On-Demand Audio Streams': 'tw_oda_streams',
            'LW On-Demand Audio Streams': 'lw_oda_streams',
            'L2W_On_Demand_Audio_Streams': 'l2w_oda_streams',
            'Weekly %change On-Demand Audio Streams': 'weekly_pct_chg_oda_streams',
            'YTD On-Demand Audio Streams': 'ytd_oda_streams',
            'RTD On-Demand Audio Streams': 'rtd_oda_streams',
            'RTD On-Demand Audio Streams - Premium': 'rtd_oda_streams_premium',
            'RTD On-Demand Audio Streams - Ad Supported': 'rtd_oda_streams_ad_supported',
            'WTD Building ODA (Friday-Thursday)': 'wtd_building_fri_thurs',
            '7-day Rolling ODA': 'tw_rolling_oda',
            'pre-7days rolling ODA': 'lw_rolling_oda',
            'TW Digital Track Sales': 'tw_digital_track_sales',
            'YTD Digital Track Sales': 'ytd_digital_track_sales',
            'ATD Digital Track Sales': 'atd_digital_track_sales',
            'TW On-Demand Video': 'tw_odv',
            'LW On-Demand Video': 'lw_odv',
            'YTD On-Demand Video': 'ytd_odv',
            'ATD On-Demand Video': 'atd_odv'
        }

        df = df.rename(columns=renameable)

        # Drop unnecessary columns
        drop_columns = [
            'lw_rank',
            'lw_oda_streams',
            'l2w_oda_streams',
            'weekly_pct_chg_oda_streams',
            'ytd_oda_streams',
            'wtd_building_fri_thurs',
            'tw_rolling_oda',
            'lw_rolling_oda',
            'ytd_digital_track_sales',
            'lw_odv',
            'ytd_odv'
        ]

        df = df.drop(columns=drop_columns)

        # Drop rows that have a null unified_song_id
        df = df[~df['unified_song_id'].isnull()].reset_index(drop=True)
        
        # Sometimes we have unified_song_id duplicates from nielsen, ignore these.
        df = df.drop_duplicates(subset='unified_song_id').reset_index(drop=True)

        # Extract the meta information separately
        meta_columns = [
            'tw_rank',
            'artist',
            'title',
            'unified_song_id',
            'label',
            'core_genre',
            'release_date',
            'isrc',
            'tw_oda_streams',
            'rtd_oda_streams',
            'rtd_oda_streams_premium',
            'rtd_oda_streams_ad_supported',
            'tw_digital_track_sales',
            'atd_digital_track_sales',
            'tw_odv',
            'atd_odv'
        ]
        meta = df[meta_columns].reset_index(drop=True)

        # Remove the meta information from the dataframe
        df = df.drop(columns=meta_columns).reset_index(drop=True)

        # Now separate all the streaming information into different dataframes
        ad_supported = df.loc[:, df.columns.str.contains('Ad Supported')].reset_index(drop=True)
        premium = df.loc[:, df.columns.str.contains('Premium')].reset_index(drop=True)
        total = df.loc[:, df.columns.str.contains('Total')].reset_index(drop=True)

        # Create schemes for renaming the columns so we can just deal with dates
        ad_supported_rename = { col: tDate(col.replace(' - Ad Supported ODA', '')) for col in ad_supported.columns }
        premium_rename = { col: tDate(col.replace(' - Premium ODA', '')) for col in premium.columns }
        total_rename = { col: tDate(col.replace(' - Total ODA', '')) for col in total.columns }

        # Rename columns so we can just deal with dates as column names
        ad_supported.rename(columns=ad_supported_rename, inplace=True)
        premium.rename(columns=premium_rename, inplace=True)
        total.rename(columns=total_rename, inplace=True)

        # Clean the types
        meta = meta.astype({
            'tw_rank': 'int',
            'artist': 'str',
            'title': 'str',
            'unified_song_id': 'int',
            'label': 'str',
            'core_genre': 'str',
            'isrc': 'str',
            'tw_oda_streams': 'int',
            'rtd_oda_streams': 'int',
            'rtd_oda_streams_premium': 'int',
            'rtd_oda_streams_ad_supported': 'int',
            'tw_digital_track_sales': 'int',
            'atd_digital_track_sales': 'int',
            'tw_odv': 'int',
            'atd_odv': 'int'
        }).astype({ 'unified_song_id': 'str' })

        # Add any additional columns we're going to need later
        meta['signed'] = False
        
        # Add the unified_song_id back onto the streaming dataframes
        total['unified_song_id'] = meta['unified_song_id']
        premium['unified_song_id'] = meta['unified_song_id']
        ad_supported['unified_song_id'] = meta['unified_song_id']
        
        return meta, total, premium, ad_supported

    def appendToSignedArtistList(self, df):
        
        # Get the signed songs from our dataset
        signed_df = df.loc[df['signed'] == True, ['artist']].reset_index(drop=True)

        # Get the existing signed artists
        signed_existing = self.db.execute('select * from misc.signed_artists')
        if signed_existing is None:
            raise Exception('Missing signed artists template')

        # Get the artists in our new df that don't exist already
        new_signed = signed_df[(~signed_df['artist'].isin(signed_existing['artist'])) & (~signed_df['artist'].isnull())].reset_index(drop=True)
        
        # Upload newly signed artists to the tracker
        self.db.big_insert(new_signed, 'misc.signed_artists')
        print(f'Inserted {new_signed.shape[0]} new signed artists to tracker...')

    def prepareSongData(self, df):
        
        # Basic cleanup and separation of datasets
        meta, total, premium, ad_supported = self.cleanSongs(df)

        # Get the date indicies for the daily streaming data
        dateCols = getDateCols(total)

        # Fill empty release dates with most recent date
        meta['release_date'] = pd.to_datetime(meta['release_date'].fillna(dateCols[0]))

        # Add a column that specifies when the report was generated
        meta['report_date'] = pd.to_datetime(dateCols[0])

        # Mark who is signed and who isn't
        meta = self.filterSignedSongs(meta)

        # Add signed artists to running list
        self.appendToSignedArtistList(meta)
        
        # Pivot all the streaming data from wide to long format
        total = total.melt(id_vars='unified_song_id', var_name='date', value_name='streams')
        ad_supported = ad_supported.melt(id_vars='unified_song_id', var_name='date', value_name='ad_supported')
        premium = premium.melt(id_vars='unified_song_id', var_name='date', value_name='premium')

        # Merge streaming info together
        streams = pd.merge(total, ad_supported, on=['unified_song_id', 'date'])
        streams = pd.merge(streams, premium, on=['unified_song_id', 'date'])

        # Make sure we don't have any null streaming values
        streams['streams']  = streams['streams'].fillna(0).astype('int')
        streams['premium']  = streams['premium'].fillna(0).astype('int')
        streams['ad_supported']  = streams['ad_supported'].fillna(0).astype('int')
        
        return meta, streams

    def songsDbUpdates(self, meta, streams):

        # META / REPORTS / ISRC UPDATES
        string = """
            create temp table tmp_meta (
                tw_rank int,
                artist text,
                title text,
                unified_song_id text,
                label text,
                core_genre text,
                release_date date,
                isrc text,
                tw_oda_streams int,
                rtd_oda_streams bigint,
                rtd_oda_streams_premium bigint,
                rtd_oda_streams_ad_supported bigint,
                tw_digital_track_sales int,
                atd_digital_track_sales int,
                tw_odv int,
                atd_odv int,
                signed boolean,
                report_date date
            );
        """
        self.db.execute(string)
        self.db.big_insert(meta, 'tmp_meta')

        string = """
            -- META
            insert into nielsen_song.meta (artist, title, unified_song_id, label, core_genre, release_date, isrc)
            select artist, title, unified_song_id, label, core_genre, release_date, isrc from tmp_meta
            on conflict (unified_song_id) do update
            set
                artist = excluded.artist,
                title = excluded.title,
                label = excluded.label,
                core_genre = excluded.core_genre,
                release_date = excluded.release_date,
                isrc = excluded.isrc,
                is_global = false;

            -- REPORTS
            insert into nielsen_song.reports (
                song_id, tw_rank, tw_oda_streams, rtd_oda_streams, tw_digital_track_sales,
                atd_digital_track_sales, tw_odv, atd_odv, signed, report_date
            )
            select
                m.id as song_id,
                tm.tw_rank,
                tm.tw_oda_streams,
                tm.rtd_oda_streams,
                tm.tw_digital_track_sales,
                tm.atd_digital_track_sales,
                tm.tw_odv,
                tm.atd_odv,
                tm.signed,
                tm.report_date
            from tmp_meta tm
            left join nielsen_song.meta m on tm.unified_song_id = m.unified_song_id;
        """
        self.db.execute(string)

        # STREAMS
        string = """
            create temp table tmp_streams (
                unified_song_id text,
                date date,
                streams int,
                ad_supported int,
                premium int
            );
        """
        self.db.execute(string)
        self.db.big_insert(streams, 'tmp_streams')

        string = """
            create temp table streams as (
                select
                    m.id as song_id,
                    ts.date,
                    ts.streams,
                    existing_streams.streams as existing_streams,
                    ts.ad_supported,
                    ts.premium,
                    case
                        when existing_streams.song_id is null then false
                        else true
                    end as record_exists
                from tmp_streams ts
                left join nielsen_song.meta m on ts.unified_song_id = m.unified_song_id
                left join (
                    select s.*
                    from nielsen_song.streams s
                    where date > now() - interval '20 days'
                        and song_id in (
                            select m.id
                            from tmp_meta tm
                            left join nielsen_song.meta m on tm.unified_song_id = m.unified_song_id
                        )
                ) existing_streams on m.id = existing_streams.song_id and ts.date = existing_streams.date
            );

            create temp table updates as (
                select
                    song_id,
                    date,
                    streams,
                    premium,
                    ad_supported
                from streams s
                where record_exists is true
                    and streams is distinct from existing_streams
            );

            create temp table inserts as (
                select
                    song_id,
                    date,
                    streams,
                    premium,
                    ad_supported
                from streams
                where record_exists is false
            );

            update nielsen_song.streams s
            set streams = updates.streams,
                ad_supported = updates.ad_supported,
                premium = updates.premium
            from updates
            where s.song_id = updates.song_id
                and s.date = updates.date;

            insert into nielsen_song.streams (song_id, date, streams, ad_supported, premium)
            select song_id, date, streams, ad_supported, premium from inserts;

            select count(*) as value, 'updates' as name from updates
            union all
            select count(*) as value, 'inserts' as name from inserts
        """
        results = self.db.execute(string)

        # Clean up
        string = """
            drop table tmp_meta;
            drop table tmp_streams;
            drop table streams;
            drop table inserts;
            drop table updates;
        """
        self.db.execute(string)

        if results is None:
            raise Exception('Error getting song updates')

        # Print the elapsed time & results
        num_inserts = results.loc[results['name'] == 'inserts', 'value'].iloc[0]
        num_updates = results.loc[results['name'] == 'updates', 'value'].iloc[0]
        print(f'Song updates: {num_inserts} inserts | {num_updates} updates')

    def processSongs(self):

        """
            Official method for processing nielsen's Song file.
        """

        # Read in the data
        df = pd.read_csv(self.fullfiles['song'], encoding='UTF-16')

        # If we're in test mode, just take a subset
        if self.settings['is_testing'] == True:
            df = df.iloc[:100].reset_index(drop=True)

        # Clean data
        meta, streams = self.prepareSongData(df)

        # Database updates
        self.songsDbUpdates(meta, streams)

    def updateRecentDate(self):

        """
            Updates the recent report date we store so we don't have to calculate it
            on the fly every time.
        """

        string = """
            update nielsen_meta
            set value = %(recent_date)s
            where id = 1
        """
        params = { 'recent_date': datetime.strftime(self.settings['date'] - timedelta(2), '%Y-%m-%d') }
        self.db.execute(string, params)

    def refreshStats(self):

        string = """
            refresh materialized view concurrently nielsen_artist.__stats;
            refresh materialized view concurrently nielsen_song.__stats;
            refresh materialized view concurrently nielsen_project.__stats;
        """
        self.db.execute(string)

    def refreshArtistTracks(self):

        string = """
            refresh materialized view concurrently nielsen_artist.__artist_tracks;
        """
        self.db.execute(string)

    def getSpotifySongs(self, df):

        """
            Takes in a dataframe with columns:
                - isrc
                - title
                - artist
        """

        def get_image(arr):
            
            if len(arr) == 0:
                return None
            else:
                arr.sort(key=lambda x: x['height'], reverse=True)
                return arr[0]['url']

        def extractSongInfo(song):
                    
            disc_number = song['disc_number']
            duration_ms = song['duration_ms']
            explicit = song['explicit']
            isrc = song['external_ids']['isrc'] if 'external_ids' in song and 'isrc' in song['external_ids'] else ''
            url = song['external_urls']['spotify'] if 'external_urls' in song and 'spotify' in song['external_urls'] else ''
            api_url = song['href']

            spotify_track_id = song['id']
            spotify_album_id = song['album']['id']
            spotify_artist_id = song['artists'][0]['id']
            
            is_local = song['is_local']
            name = song['name']
            popularity = song['popularity']
            preview_url = song['preview_url']
            track_number = song['track_number']
            uri = song['uri']
            spotify_image = get_image(song['album']['images'])
            release_date = song['album']['release_date']
            total_tracks = song['album']['total_tracks']
            album_type = song['album']['type']
            
            return {
                'disc_number': disc_number,
                'duration_ms': duration_ms,
                'explicit': explicit,
                'isrc': isrc,
                'url': url,
                'api_url': api_url,
                'spotify_track_id': spotify_track_id,
                'spotify_artist_id': spotify_artist_id,
                'spotify_album_id': spotify_album_id,
                'is_local': is_local,
                'name': name,
                'popularity': popularity,
                'preview_url': preview_url,
                'track_number': track_number,
                'uri': uri,
                'album_type': album_type,
                'spotify_image': spotify_image,
                'release_date': release_date,
                'total_tracks': total_tracks
            }

        def getSpotifyTracksManually(row, track_columns):

            # If we already have the data, skip
            if pd.notnull(row['spotify_track_id']):
                return pd.Series({ key: row[key] for key in track_columns })

            isrc = row['isrc']
            if pd.notnull(isrc):

                items = spotify.searchTracks(f'isrc:{isrc}')
            
                if len(items) > 0:
                    return pd.Series(extractSongInfo(items[0]))

            # If we don't find anything then just resort to searching by title / artist
            res = spotify.searchByTitleAndArtist(row['title'], row['artist'])
            if res is not None:
                return pd.Series(extractSongInfo(res))
            
            # If everything fails, just return all None values for the columns we expect
            return pd.Series({ key: None for key in track_columns })

        def getSpotifyTracks(df, spotify):

            """
                Gets track information about a list of spotify tracks

                Input: Dataframe with columns { title, artist, isrc }

                Steps:
                    1. Use isrcs to attempt to match to reporting_db existing cache
                    2. Use isrcs to search manually and fill in the gaps
                    3. Use title / artist to search manually and fill in the gaps
            """

            # These are the columns that will be added from this step
            track_columns = [
                'album_type',
                'api_url',
                'disc_number',
                'duration_ms',
                'explicit',
                'is_local',
                'isrc',
                'name',
                'popularity',
                'preview_url',
                'release_date',
                'spotify_album_id',
                'spotify_artist_id',
                'spotify_image',
                'spotify_track_id',
                'total_tracks',
                'track_number',
                'uri',
                'url'
            ]

            # If the dataframe is empty, just add columns and return
            if df.empty:
                df[track_columns] = None
                return df

            # Extract isrcs for bulk search
            isrcs = tuple(df.loc[~df['isrc'].isnull(), 'isrc'].unique())
            isrcs = []

            # If we found any, do a bulk search
            if len(isrcs) > 0:

                reporting_db = Db('reporting_db')
                reporting_db.connect()

                # Attempt to match the spotify track ids to the isrcs we already have
                string = """
                    select
                        isrc,
                        spotify_track_id,
                        popularity_score as popularity
                    from chartmetric_raw.spotify
                    where isrc in %(isrcs)s
                """
                params = { 'isrcs': isrcs }
                spotify = reporting_db.execute(string, params)
                if spotify is None:
                    raise Exception('Error getting from chartmetric_raw.spotify')

                # Disconnect from reporting db
                reporting_db.disconnect()

                # Remove duplicate isrcs and keep the ones with the highest popularity score
                spotify = spotify.sort_values(by='popularity', ascending=False).drop_duplicates(subset=['isrc']).drop(columns='popularity').reset_index(drop=True)

                # Merge the spotify track ids onto our main dataset
                df = pd.merge(df, spotify, on='isrc', how='left')

                # Loop through the spotify track ids we just got in bulk
                data = []
                chunks = chunker(df.loc[~df['spotify_track_id'].isnull(), 'spotify_track_id'].unique(), 50)
                for spotify_track_ids in chunks:
                    
                    # Get spotify info from api
                    tracks = spotify.tracks(spotify_track_ids)

                    # Extract information we're interested in
                    tracks = [extractSongInfo(i) for i in tracks]

                    # Append to list
                    data = [ *data, *tracks ]

                # Convert to dataframe for easy merging
                data = pd.DataFrame(data)

                # Add info to our main dataframe
                df = pd.merge(df, data, on=['isrc', 'spotify_track_id'], how='left')

            else:

                df['spotify_track_id'] = None

            # Fill in the gaps by searching for missing spotify_track_ids by isrc, and then title & artist
            df[track_columns] = df.apply(getSpotifyTracksManually, track_columns=track_columns, axis=1)

            return df

        def getSpotifyAlbums(df, spotify):

            # Converts a spotify album object to something we can digest
            def album2Data(album):

                copyrights = '/'.join([i['text'] for i in album['copyrights']])
                label = album['label']
                spotify_album_id = album['id']
                upc = album['external_ids']['upc'] if 'external_ids' in album and 'upc' in album['external_ids'] else ''
                genre = '/'.join(album['genres'])
                album_name = album['name']
                    
                return {
                    'copyrights': copyrights,
                    'label': label,
                    'spotify_album_id': spotify_album_id,
                    'upc': upc,
                    'genre': genre,
                    'album_name': album_name
                }

            # Get all the album ids
            album_ids = df.loc[(~df['spotify_album_id'].isnull()) & (df['spotify_album_id'] != ''), 'spotify_album_id'].unique().tolist()

            # Loop through in chunks of 20
            data = []
            chunks = chunker(album_ids, 20)
            for chunk in chunks:

                res = spotify.albums(chunk)

                res = [album2Data(i) for i in res]

                data = [ *data, *res ]

            # Convert this to a dataframe
            data = pd.DataFrame(data)

            # If we have nothing to return, then just return the dataframe with the columns we need as null
            if data.shape[0] == 0:
                
                df[['copyrights', 'label', 'upc', 'genre', 'album_name']] = None

            else:
                
                # Merge the copyright information onto the dataframe
                df = pd.merge(df, data, on='spotify_album_id', how='left')

            return df

        def getSpotifyAudioFeatures(df, spotify):

            # Get all the spotify track ids
            track_ids = df.loc[(~df['spotify_track_id'].isnull()) & (df['spotify_track_id'] != ''), 'spotify_track_id'].unique().tolist()

            # Loop through in chunks of 100
            data = []
            max_chunk = 100
            chunks = chunker(track_ids, max_chunk)
            for chunk in chunks:

                res = spotify.audio_features(chunk)

                data = [ *data, *res ]

            if len(data) > 0:
                
                # Convert to dataframe
                data = pd.DataFrame(data)

                # Drop columns we aren't using
                drop_columns = ['uri', 'track_href', 'duration_ms', 'type' ]
                data.drop(columns=drop_columns, inplace=True)
                
                # Rename id column for merging
                rename_columns = { 'id': 'spotify_track_id' }
                data.rename(columns=rename_columns, inplace=True)
                
                # Merge audio feature data
                df = pd.merge(df, data, on='spotify_track_id', how='left')
                
            else:
                
                df[['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'analysis_url', 'time_signature']] = None

            return df

        # Init Spotify client
        spotify = Spotify()

        df = getSpotifyTracks(df, spotify)
        df = getSpotifyAlbums(df, spotify)
        df = getSpotifyAudioFeatures(df, spotify)

        return df

    def cacheSpotifySongs(self):

        """
            Cache new information about inserted songs from nielsen song files.
        """

        # Get the songs that need to be cached
        string = """
            select id as song_id, isrc, title, artist
            from nielsen_song.meta
            where id not in (select song_id from nielsen_song.spotify)
                and is_global is false
        """
        df = self.db.execute(string)

        if df is None:
            return

        # Get the spotify info from the api
        df = self.getSpotifySongs(df)

        # Drop unnecessary columns
        df.drop(columns=['isrc', 'title', 'artist'], inplace=True)

        # Clean the types on int columns
        int_cols = ['disc_number', 'duration_ms', 'popularity', 'track_number', 'total_tracks', 'key', 'mode', 'time_signature']
        df[int_cols] = df[int_cols].fillna(0)
        df[int_cols] = df[int_cols].astype(int)

        # Sometimes dates will only have the year so we just need to add some formatting there
        mask = df['release_date'].str.len() == 4
        df.loc[mask, 'release_date'] = df[mask].apply(lambda x: x.release_date + '-01-01', axis=1)

        mask = df['release_date'].str.len() == 7
        df.loc[mask, 'release_date'] = df[mask].apply(lambda x: x.release_date + '-01', axis=1)

        # Sometimes we get these songs that have 0000 for release date, just put em on jan 1st 2000
        df.loc[df['release_date'] == '0000-01-01', 'release_date'] = '2000-01-01'

        # Insert new spotify information
        self.db.big_insert(df, 'nielsen_song.spotify')

    def bulkGetSpotifyArtistInfo(self, df, spotify):

        """
            Bulk add all the artists by spotify_artist_id
        """

        # Get all the spotify artist ids
        artist_ids = df.loc[(~df['spotify_artist_id'].isnull()) & (df['spotify_artist_id'] != ''), 'spotify_artist_id'].unique().tolist()

        # Loop through in chunks of 50
        data = []
        max_chunk = 50
        chunks = chunker(artist_ids, max_chunk)
        for chunk in chunks:

            res = spotify.artists(chunk)

            res = [transformSpotifyArtistObject(i) for i in res]

            data = [ *data, *res ]

        if len(data) > 0:
            
            # Convert to dataframe
            data = pd.DataFrame(data)
            
            # Merge audio feature data
            df = pd.merge(df, data, on='spotify_artist_id', how='left')
            
        else:
            
            df[['url', 'followers', 'genres', 'api_url', 'spotify_image', 'name', 'popularity', 'uri']] = None

        return df

    def getSpotifyArtistInfo(self, df, spotify):

        """
            Manually search artists who didn't have a spotify artist id
        """

        def transformSpotifyArtistObject2Series(obj):
            if obj is None:
                return pd.Series(tuple([None for _ in range(len(spotify_columns))]))
            else:
                return pd.Series(tuple([obj[i] for i in spotify_columns]))

        def getSpotifyArtistManually(row, spotify):
            
            if ~pd.isnull(row['spotify_artist_id']) and row['spotify_artist_id'] != '' and row['spotify_artist_id'] is not None:
                return transformSpotifyArtistObject2Series(row)

            # Refresh the spotify token every so often
            if random.random() < 0.1:
                spotify.refresh()

            name = row['artist']

            res = spotify.searchArtistByName(name)

            if res is None:

                # A lot of the time, the name is just missing a "The" at
                # the beginning, so we'll just try that
                res = spotify.searchArtistByName('The ' + name)
                
                if res is not None:
                    return transformSpotifyArtistObject2Series(transformSpotifyArtistObject(res))
                
                return pd.Series(tuple([None for _ in range(len(spotify_columns))]))

            return transformSpotifyArtistObject2Series(transformSpotifyArtistObject(res))

        spotify_columns = [
            'url',
            'followers',
            'genres',
            'api_url',
            'spotify_artist_id',
            'spotify_image',
            'name',
            'popularity',
            'uri'
        ]

        # Only do something if we have data to get
        mask = (df['spotify_artist_id'].isnull()) | (df['spotify_artist_id'] == '')
        total = df.shape[0]
        if total > 0:

            df[spotify_columns] = df.apply(getSpotifyArtistManually, spotify=spotify, axis=1)

        return df

    def getSpotifyPopularTrackId(self, df, spotify):

        """
            Attach the ids of the most popular album / track.
        """

        # Attach the album id of the artists most popular track    
        def getPopularTrackId(row, spotify):

            # We can't search if we don't have a spotify_artist_id for the artist
            if pd.isnull(row['spotify_artist_id']) or row['spotify_artist_id'] == '' or row['spotify_artist_id'] is None:
                return pd.Series((None, None))

            popular_tracks = spotify.artist_top_tracks(row['spotify_artist_id'])

            # If we didn't get anything then we can just exit
            if len(popular_tracks) == 0:
                return pd.Series((None, None))

            # Get the top tracks album id
            spotify_popular_track_id = popular_tracks[0]['id']
            spotify_popular_album_id = popular_tracks[0]['album']['id']

            # This is where we exit so that we can bulk search for album ids later
            return pd.Series((spotify_popular_track_id, spotify_popular_album_id))

        total = df.shape[0]
        cols = ['spotify_popular_track_id', 'spotify_popular_album_id']
        df[cols] = None
        if total > 0:
            df[cols] = df.apply(getPopularTrackId, spotify=spotify, axis=1)

        return df

    def getSpotifyAlbumInfo(self, df, spotify):

        """
            Takes the 'spotify_popular_track_id' column and attaches copyright info about that track
        """
        
        # Converts a spotify album object to something we can digest
        def album2Data(album):

            spotify_copyrights = '/'.join([i['text'] for i in album['copyrights']])
            spotify_label = album['label']
            spotify_popular_album_id = album['id']
            return {
                'spotify_copyrights': spotify_copyrights,
                'spotify_label': spotify_label,
                'spotify_popular_album_id': spotify_popular_album_id
            }

        # Get all the popular track album ids
        album_ids = df.loc[(pd.notnull(df['spotify_popular_album_id'])) & (df['spotify_popular_album_id'] != ''), 'spotify_popular_album_id'].unique().tolist()

        # Loop through in chunks of 20
        data = []
        chunks = chunker(album_ids, 20)
        for chunk in chunks:

            res = spotify.albums(chunk)
            res = [album2Data(i) for i in res]

            data = [ *data, *res ]

        # Convert this to a dataframe
        data = pd.DataFrame(data)

        # If we have nothing to return, then just return the dataframe with the columns we need as null
        if data.shape[0] == 0:
            
            df[['spotify_popular_album_id', 'spotify_copyrights', 'spotify_label']] = None

        else:
            
            # Merge the copyright information onto the dataframe
            df = pd.merge(df, data, on='spotify_popular_album_id', how='left')

        return df
        
    def cacheSpotifyArtists(self):

        # Get new artists inserted into the db
        string = """
            select
                distinct on (artist_id)
                artist_id,
                artist,
                spotify_artist_id
            from (
                select
                    m.artist_id,
                    m.artist,
                    m.unified_artist_id,
                    sp.spotify_artist_id,
                    count(sp.spotify_artist_id)
                from (
                    select
                        id as artist_id,
                        artist,
                        unified_artist_id
                    from nielsen_artist.meta
                    where id not in (select artist_id from nielsen_artist.spotify)
                        and is_global is false
                ) m
                left join nielsen_artist.artist_tracks art on m.artist_id = art.artist_id
                left join nielsen_song.spotify sp on sp.song_id = art.song_id
                group by
                    m.artist_id,
                    m.artist,
                    m.unified_artist_id,
                    sp.spotify_artist_id
                order by artist_id, count(sp.spotify_artist_id) desc nulls last
            ) q
            order by artist_id, count desc
        """
        df = self.db.execute(string)

        if df is None:
            raise Exception('Error getting spotify artists to cache')

        # Init spotify client
        spotify = Spotify()

        # Get spotify info
        df = self.bulkGetSpotifyArtistInfo(df, spotify)
        df = self.getSpotifyArtistInfo(df, spotify)
        df = self.getSpotifyPopularTrackId(df, spotify)
        df = self.getSpotifyAlbumInfo(df, spotify)

        # Drop down unnecessary columns
        df.drop(columns=['artist'], inplace=True)
        df[['followers', 'popularity']] = df[['followers', 'popularity']].fillna(0)
        df = df.astype({ 'followers': 'int', 'popularity': 'int' })

        # Insert into cache
        self.db.big_insert(df, 'nielsen_artist.spotify')

    def cacheChartmetricIds(self):

        """
            Cache the mapping of artist ids to their respective social
            id in chartmetrics database.

            artist_id -> instagram_id
            artist_id -> youtube_id
            artist_id -> tiktok_id
            artist_id -> spotify_id
        """

        string = """
            select s.artist_id, s.spotify_artist_id, m.unified_artist_id
            from nielsen_artist.spotify s
            left join nielsen_artist.meta m on m.id = s.artist_id
            where s.spotify_artist_id is not null
                and artist_id not in (select artist_id from nielsen_artist.cm_map)
        """
        df = self.db.execute(string)

        if df.empty:
            return

        # First extract all the spotify artist ids and drop duplicates
        spotify_artist_ids = df[['spotify_artist_id']].drop_duplicates(subset=['spotify_artist_id']).reset_index(drop=True)

        reporting_db = Db('reporting_db')
        reporting_db.connect()

        string = """
            create temp table ids (
                spotify_artist_id text
            );
        """
        reporting_db.execute(string)
        reporting_db.big_insert_redshift(spotify_artist_ids, 'ids')

        string = """
            with t as (
                select
                    ids.spotify_artist_id,
                    sa.cm_artist as target_id,
                    cm.account_id,
                    cm.type
                from ids
                join chartmetric_raw.spotify_artist sa on ids.spotify_artist_id = sa.spotify_artist_id
                left join chartmetric_raw.cm_url cm
                    on cm.target_id = sa.cm_artist
                    and cm.target = 'cm_artist'
            ), instagram as (
                select target_id, account_id as instagram_id
                from t where type = 2
            ), youtube as (
                select target_id, account_id as youtube_id
                from t where type = 3
            ), tiktok as (
                select target_id, account_id as tiktok_id
                from t where type = 19
            ), shazam as (
                select target_id, account_id as shazam_id
                from t where type = 16
            ), twitter as (
                select target_id, account_id as twitter_id
                from t where type = 1
            ), genius as (
                select target_id, account_id as genius_id
                from t where type = 17
            ), gtrends as (
                select target_id, account_id as gtrends_id
                from t where type = 6
            ), soundcloud as (
                select target_id, account_id as soundcloud_id
                from t where type = 7
            ), twitch as (
                select target_id, account_id as twitch_id
                from t where type = 20
            )

            select
                t.target_id,
                t.spotify_artist_id,
                ig.instagram_id,
                yt.youtube_id,
                tt.tiktok_id,
                sz.shazam_id,
                tw.twitter_id,
                gn.genius_id,
                gt.gtrends_id,
                sc.soundcloud_id,
                tc.twitch_id,
                sa.id as spotify_id
            from t
            left join instagram ig on t.target_id = ig.target_id
            left join youtube yt on t.target_id = yt.target_id
            left join tiktok tt on t.target_id = tt.target_id
            left join shazam sz on t.target_id = sz.target_id
            left join twitter tw on t.target_id = tw.target_id
            left join genius gn on t.target_id = gn.target_id
            left join gtrends gt on t.target_id = gt.target_id
            left join soundcloud sc on t.target_id = sc.target_id
            left join twitch tc on t.target_id = tc.target_id
            left join chartmetric_raw.spotify_artist sa on t.spotify_artist_id = sa.spotify_artist_id
            group by
                t.target_id, t.spotify_artist_id,
                sa.id,
                instagram_id, youtube_id, tiktok_id,
                shazam_id, twitter_id, genius_id, gtrends_id, soundcloud_id, twitch_id
        """
        data = reporting_db.execute(string)
        reporting_db.disconnect()

        if data.empty:
            return

        # Clean the potentially int columns of the notorious .0
        data = data.astype({
            'target_id': 'str',
            'spotify_id': 'str'
        })

        data[['target_id', 'spotify_id']] = data[['target_id', 'spotify_id']].replace('.0', '', regex=True)

        # Merge back with original data
        df = pd.merge(df, data, on='spotify_artist_id', how='left')

        # Create temp table for the new ids we're going to insert
        string = """
            create temp table tmp_ids (
                artist_id int,
                spotify_artist_id text,
                unified_artist_id text,
                target_id text,
                instagram_id text,
                youtube_id text,
                tiktok_id text,
                shazam_id text,
                twitter_id text,
                genius_id text,
                gtrends_id text,
                soundcloud_id text,
                twitch_id text,
                spotify_id text
            )
        """
        self.db.execute(string)
        self.db.big_insert(df, 'tmp_ids')

        # Upsert new ids
        string = """
            insert into nielsen_artist.cm_map (
                artist_id, spotify_artist_id, unified_artist_id, target_id, instagram_id, youtube_id,
                tiktok_id, shazam_id, twitter_id, genius_id, gtrends_id, soundcloud_id, twitch_id, spotify_id
            )
            select
                artist_id, spotify_artist_id, unified_artist_id, target_id, instagram_id, youtube_id,
                tiktok_id, shazam_id, twitter_id, genius_id, gtrends_id, soundcloud_id, twitch_id, spotify_id
            from tmp_ids
            on conflict (artist_id) do update
            set
                spotify_artist_id = excluded.spotify_artist_id,
                unified_artist_id = excluded.unified_artist_id,
                target_id = excluded.target_id,
                instagram_id = excluded.instagram_id,
                youtube_id = excluded.youtube_id,
                tiktok_id = excluded.tiktok_id,
                shazam_id = excluded.shazam_id,
                twitter_id = excluded.twitter_id,
                genius_id = excluded.genius_id,
                gtrends_id = excluded.gtrends_id,
                soundcloud_id = excluded.soundcloud_id,
                twitch_id = excluded.twitch_id,
                spotify_id = excluded.spotify_id
        """
        self.db.execute(string)

    def insertNewGenres(self):

        string = """
            create temp table tmp_new_genres (genre) as (
                select all_genres.genre
                from (
                    select
                        distinct on (genre)
                        genre
                    from nielsen_artist.spotify sp,
                        unnest(string_to_array(sp.genres, '/')) s(genre)
                ) all_genres
                left join nielsen_genres.meta m on m.genre = all_genres.genre
                where m.id is null
            );

            insert into nielsen_genres.meta (genre)
            select genre from tmp_new_genres;

            select count(*) as num_new_genres from tmp_new_genres;
        """
        new_genres = self.db.execute(string)
        num_new_genres = new_genres.loc[0, 'num_new_genres'] if new_genres is not None else 0
        print(f'{num_new_genres} new genres inserted')

    def insertNewGenreStreams(self):

        string = """
            delete from nielsen_genres.streams
            where streams.date > ( select value::date - interval '7 days' as date from nielsen_meta where id = 1 );

            insert into nielsen_genres.streams (genre_id, genre, streams, date)
            select
                m.id as genre_id,
                s.genre,
                s.streams,
                s.date
            from (
                select
                    genre,
                    streams.date,
                    sum(streams.streams) as streams
                from nielsen_artist.spotify sp
                left join nielsen_artist.streams on sp.artist_id = streams.artist_id,
                unnest(string_to_array(sp.genres, '/')) s(genre)
                where streams.date > ( select value::date - interval '7 days' as date from nielsen_meta where id = 1 )
                group by genre, date
            ) s
            left join nielsen_genres.meta m on s.genre = m.genre
        """
        self.db.execute(string)

    def refreshGenreCorrelations(self):
        
        string = """
            refresh materialized view concurrently nielsen_genres.__correlations;
        """
        self.db.execute(string)

    def refreshGenreStats(self):

        string = """
            refresh materialized view concurrently nielsen_genres.__stats;
        """
        self.db.execute(string)

    def refreshGenreArtists(self):

        string = """
            refresh materialized view concurrently nielsen_genres.__artists;
        """
        self.db.execute(string)

    def refreshGenreSparklines(self):

        string = """
            refresh materialized view concurrently nielsen_genres.__sparklines;
        """
        self.db.execute(string)

    def updateGenres(self):

        """
            Update all information about genres that connects artist data
            with spotify genres.
        """

        self.insertNewGenres()
        self.insertNewGenreStreams()
        self.refreshGenreStats()
        self.refreshGenreCorrelations()
        self.refreshGenreArtists()
        self.refreshGenreSparklines()

    def cacheSpotifyAlbums(self):

        """
            Cache information about spotify albums relavent to our database.
        """

        # Converts a spotify album object to something we can digest
        def album2Data(album):

            copyrights = '/'.join([i['text'] for i in album['copyrights']])
            label = album['label']
            spotify_album_id = album['id']
            genres = ','.join(album['genres'])
            spotify_image = album['images'][0]['url'] if len(album['images']) > 0 else ''
            href = album['href']
            release_date = album['release_date']
            popularity = album['popularity']
            name = album['name']
            
            return {
                'copyrights': copyrights,
                'label': label,
                'spotify_album_id': spotify_album_id,
                'genres': genres,
                'spotify_image': spotify_image,
                'href': href,
                'release_date': release_date,
                'popularity': popularity,
                'name': name
            }
            
        # Get a batch of spotify album ids to cache from the songs db
        string = """
            select distinct spotify_album_id
            from nielsen_song.spotify
            where spotify_album_id not in (
                select distinct spotify_album_id
                from spotify_albums
                where spotify_album_id is not null
                    and length(spotify_album_id) > 0
            )
            and length(spotify_album_id) > 0
        """
        df = self.db.execute(string)

        if df is None or df.empty:
            return

        # Init spotify client
        spotify = Spotify()

        # Preset (Loop through in chunks of 20)
        data = []
        album_ids = df['spotify_album_id'].tolist()
        chunks = chunker(album_ids, 20)
        for chunk in chunks:

            res = spotify.albums(chunk)
            res = [album2Data(i) for i in res]

            data = [ *data, *res ]
            
        # Convert this to a dataframe
        df = pd.DataFrame(data)

        # Sometimes dates will only have the year so we just need to add some formatting there
        mask = df['release_date'].str.len() == 4
        df.loc[mask, 'release_date'] = df[mask].apply(lambda x: x.release_date + '-01-01', axis=1)

        mask = df['release_date'].str.len() == 7
        df.loc[mask, 'release_date'] = df[mask].apply(lambda x: x.release_date + '-01', axis=1)

        df.loc[df['release_date'] == '0000-01-01', 'release_date'] = '2000-01-01'

        # Insert into db
        self.db.big_insert(df, 'spotify_albums')

    def filterSignedFromSpotifyCopyrights(self):

        """
            Use the spotify copyrights to filter signed artists
        """

        def aaronMethodNielsenLabelsSignedToSongFilter(row, labels):
        
            # Extract values
            df_label = row['spotify_copyrights'].lower()
            
            # Find matches
            res = [ele for ele in labels if(ele in df_label)]
            res = bool(res)
            
            if res == True:
                return True
            else:
                return False

        def aaronMethodNielsenLabelsSignedToSong(df, db):
            
            # Load in nielsen_labels
            nielsen_labels = db.execute('select * from misc.list_of_labels')
            labels = [i.lower() for i in nielsen_labels['label'].values]
            
            df['signed'] = df.apply(aaronMethodNielsenLabelsSignedToSongFilter, labels=labels, axis=1)
            
            return df

        def filterSigned(df, db):

            df = aaronMethodNielsenLabelsSignedToSong(df, db)
            return df[df['signed'] == True].reset_index(drop=True)

        # Get artists to check
        string = """
            select
                r.artist_id,
                r.report_date,
                m.artist,
                sp.spotify_copyrights,
                signed
            from (
                select *
                from nielsen_artist.reports
                where report_date = (select value::date from nielsen_meta where id = 1)
            ) r
            left join nielsen_artist.spotify sp on r.artist_id = sp.artist_id
            left join nielsen_artist.meta m on r.artist_id = m.id
            where signed = false
                and sp.spotify_copyrights is not null
        """
        artists = self.db.execute(string)

        # Get songs to check
        string = """
            select
                r.song_id,
                m.title,
                m.artist,
                r.report_date,
                sp.copyrights as spotify_copyrights,
                signed
            from (
                select *
                from nielsen_song.reports
                where report_date = (select value::date from nielsen_meta where id = 1)
            ) r
            left join nielsen_song.spotify sp on r.song_id = sp.song_id
            left join nielsen_song.meta m on r.song_id = m.id
            where signed = false
                and sp.copyrights is not null
        """
        songs = self.db.execute(string)

        # Perform filter
        artists = filterSigned(artists, self.db).drop(columns=['artist', 'spotify_copyrights', 'signed'])
        songs = filterSigned(songs, self.db).drop(columns=['artist', 'title', 'spotify_copyrights', 'signed'])

        # Create temp tables
        string = """
            create temp table tmp_artists_signed (
                artist_id int,
                report_date date
            );

            create temp table tmp_songs_signed (
                song_id int,
                report_date date
            );
        """
        self.db.execute(string)
        self.db.big_insert(artists, 'tmp_artists_signed')
        self.db.big_insert(songs, 'tmp_songs_signed')

        # Make updates
        string = """
            -- Update artists
            update nielsen_artist.reports r
            set signed = true
            from tmp_artists_signed tas
            where r.artist_id = tas.artist_id
                and r.report_date = tas.report_date;

            -- Update songs
            update nielsen_song.reports r
            set signed = true
            from tmp_songs_signed tss
            where r.song_id = tss.song_id
                and r.report_date = tss.report_date;

            -- Cleanup
            drop table tmp_artists_signed;
            drop table tmp_songs_signed;
        """
        self.db.execute(string)

    def refreshReportsRecent(self):

        string = """
            refresh materialized view concurrently nielsen_artist.__reports_recent;
            refresh materialized view concurrently nielsen_song.__reports_recent;
        """
        self.db.execute(string)

    def refreshCharts(self):

        """
            Keep track of all the genres and artists and make records of new things that
            enter these so that we can build notifications from them.
        """

        string = """
            create temp table cd as ( select value::date as cd from nielsen_meta where id = 1 );

            -- Long term growth at the genre level
            insert into nielsen_genres.ltg_chart (genre_id, num_positive_weeks, rnk, is_top_20, is_top_100, should_notify, last_appearance_in_top_20, last_appearance_in_top_100)
            select
                genre_id,
                num_positive_weeks,
                rnk,
                is_top_20,
                is_top_100,
                case
                    when should_notify + 1 > 7 then 0
                    when should_notify > 0 or (is_top_20 and ( select cd from cd ) - last_appearance_in_top_20 >= interval '365 days') or (is_top_100 and ( select cd from cd ) - last_appearance_in_top_100 >= interval '365 days') then should_notify + 1
                    else 0
                end as should_notify,
                case
                    when is_top_20 is true then ( select cd from cd )
                    else last_appearance_in_top_20
                end as last_appearance_in_top_20,
                case
                    when is_top_100 is true then ( select cd from cd )
                    else last_appearance_in_top_100
                end as last_appearance_in_top_100
            from (
                select
                    q.*,
                    case
                        when q.rnk < 21 then true
                        else false
                    end as is_top_20,
                    case
                        when q.rnk < 101 then true
                        else false
                    end as is_top_100,
                    coalesce(c.should_notify, 0) as should_notify,
                    coalesce(c.last_appearance_in_top_100, ( select cd from cd ) - interval '1000 days') as last_appearance_in_top_100,
                    coalesce(c.last_appearance_in_top_20, ( select cd from cd ) - interval '1000 days') as last_appearance_in_top_20
                from (
                    select
                        m.id as genre_id,
                        coalesce(num_positive_weeks, -1) as num_positive_weeks,
                        row_number() over (order by num_positive_weeks desc nulls last) as rnk
                    from (
                        select
                            genre_id,
                            sum(is_positive) as num_positive_weeks
                        from (
                            select
                                genre_id,
                                case
                                    when streams > lag_streams then 1
                                    else 0
                                end as is_positive
                            from (
                                select *, lag(streams, 1) over (partition by genre_id order by weekly) as lag_streams
                                from (
                                    select
                                        genre_id,
                                        date_trunc('week', date) as weekly,
                                        sum(streams) as streams
                                    from (
                                        select
                                            s.genre_id,
                                            date - ( select extract(dow from cd - interval '1 day')::int as number_of_days from cd ) as date,
                                            streams
                                        from nielsen_genres.streams s
                                        where date > ( select cd from cd ) - interval '365 days'
                                            and date < ( select cd from cd )
                                    ) q
                                    group by genre_id, weekly
                                ) q
                            ) q
                        ) q
                        group by genre_id
                    ) q
                    full outer join nielsen_genres.meta m on q.genre_id = m.id
                ) q
                left join nielsen_genres.ltg_chart c on q.genre_id = c.genre_id
            ) q
            on conflict (genre_id) do update
            set
                num_positive_weeks = excluded.num_positive_weeks,
                rnk = excluded.rnk,
                should_notify = excluded.should_notify,
                is_top_20 = excluded.is_top_20,
                is_top_100 = excluded.is_top_100,
                last_appearance_in_top_20 = excluded.last_appearance_in_top_20,
                last_appearance_in_top_100 = excluded.last_appearance_in_top_100;

            -- Based on weekly streams by artist at the genre level
            insert into nielsen_genres.artist_tw_chart (artist_id, genre_id, tw_streams, rnk, is_top_10, is_top_50, should_notify, last_appearance_in_top_10, last_appearance_in_top_50)
            select
                artist_id,
                genre_id,
                tw_streams,
                rnk,
                is_top_10,
                is_top_50,
                case
                    when should_notify + 1 > 3 then 0
                    when should_notify > 0 or (is_top_10 and ( select cd from cd ) - last_appearance_in_top_10 >= interval '365 days') or (is_top_50 and ( select cd from cd ) - last_appearance_in_top_50 >= interval '365 days') then should_notify + 1
                    else 0
                end as should_notify,
                case
                    when is_top_10 is true then ( select cd from cd )
                    else last_appearance_in_top_10
                end as last_appearance_in_top_10,
                case
                    when is_top_50 is true then ( select cd from cd )
                    else last_appearance_in_top_50
                end as last_appearance_in_top_50
            from (
                select
                    a.artist_id,
                    a.genre_id,
                    m.tw_streams,
                    a.rnk,
                    case
                        when a.rnk < 11 then true
                        else false
                    end as is_top_10,
                    case
                        when a.rnk < 51 then true
                        else false
                    end as is_top_50,
                    coalesce(c.should_notify, 0) as should_notify,
                    coalesce(c.last_appearance_in_top_50, ( select cd from cd ) - interval '1000 days') as last_appearance_in_top_50,
                    coalesce(c.last_appearance_in_top_10, ( select cd from cd ) - interval '1000 days') as last_appearance_in_top_10
                from nielsen_genres.artists a
                left join nielsen_artist.__artist m on a.artist_id = m.artist_id
                left join nielsen_genres.artist_tw_chart c on a.artist_id = c.artist_id and a.genre_id = c.genre_id
            ) q
            on conflict (artist_id, genre_id) do update
            set
                tw_streams = excluded.tw_streams,
                rnk = excluded.rnk,
                should_notify = excluded.should_notify,
                is_top_10 = excluded.is_top_10,
                is_top_50 = excluded.is_top_50,
                last_appearance_in_top_10 = excluded.last_appearance_in_top_10,
                last_appearance_in_top_50 = excluded.last_appearance_in_top_50;

            -- Drop the temporary date table
            drop table cd;
        """
        self.db.execute(string)

    def refreshDailyReport(self):

        string = """
            refresh materialized view concurrently nielsen_song.__daily_report;
        """
        self.db.execute(string)

    def refreshSimpleViews(self):

        string = """
            refresh materialized view concurrently nielsen_artist.__artist;
            refresh materialized view concurrently nielsen_song.__song;
        """
        self.db.execute(string)

    def refreshNotifications(self):

        string = """
            delete from graphitti.notifications;

            -- Genre long term growth
            insert into graphitti.notifications (user_id, n_type, meta)
            select
                user_id,
                n_type,
                json_build_object (
                    'genre_id', genre_id,
                    'genre', genre,
                    'sparkline', sparkline,
                    'tw_streams', tw_streams
                ) as meta
            from (
                select
                    u.id as user_id,
                    c.genre_id,
                    'new_genre_ltg' as n_type,
                    m.genre,
                    st.tw_streams,
                    json_agg (
                        json_build_object (
                            'date', spk.date,
                            'value', spk.streams
                        )
                    ) as sparkline
                from graphitti.users u
                cross join nielsen_genres.ltg_chart c
                left join nielsen_genres.sparklines spk on c.genre_id = spk.genre_id
                left join nielsen_genres.meta m on c.genre_id = m.id
                left join nielsen_genres.__stats st on c.genre_id = st.genre_id
                where should_notify::bool is true
                group by u.id, c.genre_id, m.genre, st.tw_streams
            ) q;
        """
        self.db.execute(string)

    def updateArtistSocialCharts(self):

        def get_ids(df, col):

            # First drop duplicates
            df = df.drop_duplicates(subset=[col]).reset_index(drop=True)

            # Remove null values and empty strings
            mask = (
                (~df[col].isnull()) &
                (df[col].str.len() > 0)
            )

            df = df[mask].reset_index(drop=True)

            return df[[col]]

        def updateInstagramChart(df):

            # Get the instagram data from reporting db
            instagram_ids = get_ids(df, 'instagram_id')

            string = """
                create temp table tmp_instagram_ids (
                    instagram_id text
                );
            """
            reporting_db.execute(string)
            reporting_db.big_insert_redshift(instagram_ids, 'tmp_instagram_ids')

            string = """
                select
                    account_id as instagram_id,
                    timestp as date,
                    followers as ig_followers
                from tmp_instagram_ids ig
                join chartmetric_raw.instagram_stat igs on ig.instagram_id = igs.account_id
                where date > dateadd('days', -16, current_date)
            """
            xdf = reporting_db.execute(string)

            string = 'drop table tmp_instagram_ids'
            reporting_db.execute(string)

            # Some preprocessing so that we can work with a clean dataset during our actual analysis

            # Remove null values in followers column
            xdf = xdf[~xdf['ig_followers'].isnull()].reset_index(drop=True)

            # Clean types
            xdf['date'] = pd.to_datetime(xdf['date'])
            xdf = xdf.astype({
                'instagram_id': 'str',
                'ig_followers': 'int'
            })

            # Sometimes chartmetric has duplicates for certain days, so let's drop those
            xdf = xdf.drop_duplicates(subset=['instagram_id', 'date']).reset_index(drop=True)

            # We're going to add a new id as an integer column because ints work faster with grouping
            ids = xdf.drop_duplicates(subset=['instagram_id']).reset_index(drop=True)
            ids = ids.reset_index().rename(columns={ 'index': 'instagram_id_int' }).drop(columns=['date', 'ig_followers'])
            xdf = pd.merge(xdf, ids, on='instagram_id', how='left')

            # Interpolate missing data
            xdf = xdf.set_index('date')
            xdf = xdf.groupby('instagram_id_int').resample('D').ig_followers.mean().reset_index()
            xdf['ig_followers'] = xdf['ig_followers'].interpolate()

            # Add back the original id
            xdf = pd.merge(xdf, ids, on='instagram_id_int', how='left')

            # Remove anyone with less than min_ig_followers (could use the groupby method but this is faster for this type of computation)
            min_ig_followers = 5000
            ig_followers = xdf.sort_values(by=['instagram_id_int', 'date'], ascending=False).drop_duplicates(subset='instagram_id_int', keep='first').reset_index(drop=True)
            ig_followers = ig_followers[ig_followers['ig_followers'] > min_ig_followers].reset_index(drop=True)
            xdf = pd.merge(xdf, ig_followers[['instagram_id_int']], how='inner')

            # Filter out anyone without at least 4 days of available data
            xdf = xdf.groupby('instagram_id_int').filter(lambda x: len(x) > 3)

            # Add the max date per group as a column (because we're going to filter out any points
            # that aren't the most recent datapoint & the datapoint the week prior)
            max_date = xdf.groupby('instagram_id_int').date.max().rename('max_date').reset_index()
            xdf = pd.merge(xdf, max_date, on='instagram_id_int')

            # Drop any rows that aren't the most recent 2 weeks
            mask = (
                (xdf['date'] == xdf['max_date']) | \
                (xdf['date'] == xdf['max_date'] - timedelta(7))
            )
            xdf = xdf[mask].drop(columns=['max_date']).reset_index(drop=True)

            # Drop anything that doesn't have at least 2 rows because that means that it didn't have
            # any data from the previous week so we'll just disregard it
            xdf = xdf.groupby('instagram_id_int').filter(lambda x: len(x) == 2)

            # Get the follower increase from the previous week
            xdf = xdf.sort_values(by=['instagram_id_int', 'date']).reset_index(drop=True)
            xdf['gain'] = xdf.groupby('instagram_id_int').ig_followers.diff()
            xdf = xdf[~xdf['gain'].isnull()].drop(columns=['date']).reset_index(drop=True)

            # Apply our instagram model
            model = BinnedModel('ig_gain_model')
            xdf = model.fit(xdf, 'ig_followers', 'gain')

            # Remove anything that isn't at least +2std above mean and sort by z score
            xdf = xdf[xdf['gain'] > xdf['mean'] + xdf['std'] * 2].sort_values(by='z-score', ascending=False).reset_index(drop=True)

            # Document prep

            # Drop/rename columns
            xdf.drop(columns=['mean', 'std', 'instagram_id_int'], inplace=True)
            xdf.rename(columns={ 'z-score': 'ig_growth_score', 'gain': 'ig_followers_gained' }, inplace=True)

            # Add the metadata back onto the dataframe
            # Only keep the instagram ids of the artist_id reference that has the most tw streams because that's likely the one that we want to match to
            metadata = df.sort_values(by=['instagram_id', 'tw_streams'], ascending=False).drop_duplicates(subset=['instagram_id'], keep='first').reset_index(drop=True)
            xdf = pd.merge(xdf, metadata, on='instagram_id', how='left')

            # Round these columns
            xdf['ig_followers'] = xdf['ig_followers'].astype(int)
            xdf['ig_followers_gained'] = xdf['ig_followers_gained'].astype(int)

            # Organize
            xdf = xdf[['artist_id', 'instagram_id', 'artist', 'ig_followers', 'ig_followers_gained', 'ig_growth_score', 'tw_streams', 'lw_streams', 'pct_chg']]

            # Upload the data to our db
            # First match to the existing chart to find anything that's new
            string = """
                create temp table tmp_ig_follower_growth (
                    artist_id bigserial,
                    instagram_id text,
                    artist text,
                    ig_followers int,
                    ig_followers_gained int,
                    ig_growth_score float,
                    tw_streams int,
                    lw_streams int,
                    pct_chg float
                );
            """
            self.db.execute(string)
            self.db.big_insert(xdf, 'tmp_ig_follower_growth')

            string = """
                create temp table tmp_data as (
                    select
                        t.artist_id,
                        t.instagram_id,
                        t.artist,
                        t.ig_followers,
                        t.ig_followers_gained,
                        t.ig_growth_score,
                        t.tw_streams,
                        t.lw_streams,
                        t.pct_chg,
                        case
                            when g.artist_id is null then true
                            else false
                        end as is_new
                    from tmp_ig_follower_growth t
                    left join social_charts.ig_follower_growth g on t.artist_id = g.artist_id
                );

                delete from social_charts.ig_follower_growth;

                insert into social_charts.ig_follower_growth (artist_id, instagram_id, artist, ig_followers, ig_followers_gained, ig_growth_score, tw_streams, lw_streams, pct_chg, is_new)
                select
                    artist_id,
                    instagram_id,
                    artist,
                    ig_followers,
                    ig_followers_gained,
                    ig_growth_score,
                    tw_streams,
                    lw_streams,
                    pct_chg,
                    is_new
                from tmp_data;

                drop table tmp_data;
                drop table tmp_ig_follower_growth;
            """
            self.db.execute(string)

        def updateSpotifyChart(df):

            # Get the spotify data from reporting db
            spotify_ids = get_ids(df, 'spotify_id')

            # Copy ids into a temp table in the redshift db
            string = """
                create temp table tmp_spotify_ids (
                    spotify_id text
                );
            """
            reporting_db.execute(string)
            reporting_db.big_insert_redshift(spotify_ids, 'tmp_spotify_ids')

            # Join and select instagram data from redshift db
            string = """
                select
                    spotify_artist as spotify_id,
                    timestp as date,
                    followers as sp_followers
                from tmp_spotify_ids tsi
                join chartmetric_raw.spotify_artist sa on tsi.spotify_id = sa.id
                join chartmetric_raw.spotify_artist_stat sas on tsi.spotify_id = sas.spotify_artist
                where sa.followers_latest > 2000
                    and date > dateadd('days', -16, current_date)
            """
            xdf = reporting_db.execute(string)

            # Drop the temporary table to stay clean
            string = 'drop table tmp_spotify_ids'
            reporting_db.execute(string)

            # Some preprocessing so that we can work with a clean dataset during our actual analysis

            # Remove null values in followers column
            xdf = xdf[~xdf['sp_followers'].isnull()].reset_index(drop=True)

            # Clean types
            xdf['date'] = pd.to_datetime(xdf['date'])
            xdf = xdf.astype({
                'spotify_id': 'int',
                'sp_followers': 'int'
            })

            # Sometimes chartmetric has duplicates for certain days, so let's drop those
            xdf = xdf.drop_duplicates(subset=['spotify_id', 'date']).reset_index(drop=True)

            # Filter out anyone without at least 5 days of available data
            xdf = xdf.groupby('spotify_id').filter(lambda x: len(x) > 4)

            # Interpolate missing data
            xdf = xdf.set_index('date')
            xdf = xdf.groupby('spotify_id').resample('D').sp_followers.mean().reset_index()
            xdf['sp_followers'] = xdf['sp_followers'].interpolate()

            # Add the max date per group as a column (because we're going to filter out any points
            # that aren't the most recent datapoint & the datapoint the week prior)
            max_date = xdf.groupby('spotify_id').date.max().rename('max_date').reset_index()
            xdf = pd.merge(xdf, max_date, on='spotify_id')

            # Drop any rows that aren't the most recent 2 weeks
            mask = (
                (xdf['date'] == xdf['max_date']) | \
                (xdf['date'] == xdf['max_date'] - timedelta(7))
            )
            xdf = xdf[mask].drop(columns=['max_date']).reset_index(drop=True)

            # Drop anything that doesn't have at least 2 rows because that means that it didn't have
            # any data from the previous week so we'll just disregard it
            xdf = xdf.groupby('spotify_id').filter(lambda x: len(x) == 2)

            # Get the follower increase from the previous week
            xdf = xdf.sort_values(by=['spotify_id', 'date']).reset_index(drop=True)
            xdf['gain'] = xdf.groupby('spotify_id').sp_followers.diff()
            xdf = xdf[~xdf['gain'].isnull()].drop(columns=['date']).reset_index(drop=True)

            # Get and apply our model
            model = BinnedModel('spotify_gain_model')
            xdf = model.fit(xdf, 'sp_followers', 'gain')

            # Remove anything that isn't at least +2.5std above mean and sort by z score
            # This is a slightly higher threshold because we have a much wider net because we have a lot more spotify ids
            xdf = xdf[xdf['gain'] > xdf['mean'] + xdf['std'] * 2.5].sort_values(by='z-score', ascending=False).reset_index(drop=True)

            # Drop/rename columns
            xdf.drop(columns=['mean', 'std'], inplace=True)
            xdf.rename(columns={ 'z-score': 'sp_growth_score', 'gain': 'sp_followers_gained' }, inplace=True)

            # Add the metadata back onto the dataframe
            # Only keep the spotify ids of the artist_id reference that has the most tw streams because that's likely the one that we want to match to
            metadata = df.sort_values(by=['spotify_id', 'tw_streams'], ascending=False) \
                .drop_duplicates(subset=['spotify_id'], keep='first') \
                .reset_index(drop=True)
            xdf = xdf.astype({ 'spotify_id': 'str' }) # need to do this to match merging types
            xdf = pd.merge(xdf, metadata, on='spotify_id', how='left')

            # Round these columns
            xdf['sp_followers'] = xdf['sp_followers'].astype(int)
            xdf['sp_followers_gained'] = xdf['sp_followers_gained'].astype(int)

            # Organize
            xdf = xdf[['artist_id', 'spotify_id', 'artist', 'sp_followers', 'sp_followers_gained', 'sp_growth_score', 'tw_streams', 'lw_streams', 'pct_chg']]

            # Upload the data to our db
            # First match to the existing chart to find anything that's new
            string = """
                create temp table tmp_sp_follower_growth (
                    artist_id bigserial,
                    spotify_id text,
                    artist text,
                    sp_followers int,
                    sp_followers_gained int,
                    sp_growth_score float,
                    tw_streams int,
                    lw_streams int,
                    pct_chg float
                );
            """
            self.db.execute(string)
            self.db.big_insert(xdf, 'tmp_sp_follower_growth')

            string = """
                create temp table tmp_data as (
                    select
                        t.artist_id,
                        t.spotify_id,
                        t.artist,
                        t.sp_followers,
                        t.sp_followers_gained,
                        t.sp_growth_score,
                        t.tw_streams,
                        t.lw_streams,
                        t.pct_chg,
                        case
                            when g.artist_id is null then true
                            else false
                        end as is_new
                    from tmp_sp_follower_growth t
                    left join social_charts.sp_follower_growth g on t.artist_id = g.artist_id
                );

                delete from social_charts.sp_follower_growth;

                insert into social_charts.sp_follower_growth (artist_id, spotify_id, artist, sp_followers, sp_followers_gained, sp_growth_score, tw_streams, lw_streams, pct_chg, is_new)
                select
                    artist_id,
                    spotify_id,
                    artist,
                    sp_followers,
                    sp_followers_gained,
                    sp_growth_score,
                    tw_streams,
                    lw_streams,
                    pct_chg,
                    is_new
                from tmp_data;

                drop table tmp_data;
                drop table tmp_sp_follower_growth;
            """
            self.db.execute(string)
        
        def updateTiktokChart(df):

            # Get the instagram data from reporting db
            tiktok_ids = get_ids(df, 'tiktok_id')

            # Copy ids into a temp table in the redshift db
            string = """
                create temp table tmp_tiktok_ids (
                    tiktok_id text
                );
            """
            reporting_db.execute(string)
            reporting_db.big_insert_redshift(tiktok_ids, 'tmp_tiktok_ids')

            # Join and select instagram data from redshift db
            string = """
                select
                    tu.user_id as tiktok_id,
                    timestp as date,
                    followers as tt_followers
                from tmp_tiktok_ids t
                join chartmetric_raw.tiktok_user tu on t.tiktok_id = tu.user_id
                join chartmetric_raw.tiktok_user_stat tus on tu.id = tus.tiktok_user
                where tu.followers_latest > 1000
                    and tus.followers != 0
                    and date > dateadd('days', -21, current_date)
            """
            xdf = reporting_db.execute(string)

            # Drop the temporary table to stay clean
            string = 'drop table tmp_tiktok_ids'
            reporting_db.execute(string)

            # Some preprocessing so that we can work with a clean dataset during our actual analysis

            # Remove null values in followers column
            xdf = xdf[~xdf['tt_followers'].isnull()].reset_index(drop=True)

            # Clean types
            xdf['date'] = pd.to_datetime(xdf['date'])
            xdf = xdf.astype({
                'tiktok_id': 'int',
                'tt_followers': 'int'
            })

            # Sometimes chartmetric has duplicates for certain days, so let's drop those
            xdf = xdf.drop_duplicates(subset=['tiktok_id', 'date']).reset_index(drop=True)

            # Filter out anyone without at least 5 days of available data
            xdf = xdf.groupby('tiktok_id').filter(lambda x: len(x) > 4)

            # Interpolate missing data
            xdf = xdf.set_index('date')
            xdf = xdf.groupby('tiktok_id').resample('D').tt_followers.mean().reset_index()
            xdf['tt_followers'] = xdf['tt_followers'].interpolate()

            # Add the max date per group as a column (because we're going to filter out any points
            # that aren't the most recent datapoint & the datapoint the week prior)
            max_date = xdf.groupby('tiktok_id').date.max().rename('max_date').reset_index()
            xdf = pd.merge(xdf, max_date, on='tiktok_id')

            # Drop any rows that aren't the most recent 2 weeks
            mask = (
                (xdf['date'] == xdf['max_date']) | \
                (xdf['date'] == xdf['max_date'] - timedelta(7))
            )
            xdf = xdf[mask].drop(columns=['max_date']).reset_index(drop=True)

            # Drop anything that doesn't have at least 2 rows because that means that it didn't have
            # any data from the previous week so we'll just disregard it
            xdf = xdf.groupby('tiktok_id').filter(lambda x: len(x) == 2)

            # Get the follower increase from the previous week
            xdf = xdf.sort_values(by=['tiktok_id', 'date']).reset_index(drop=True)
            xdf['gain'] = xdf.groupby('tiktok_id').tt_followers.diff()
            xdf = xdf[~xdf['gain'].isnull()].drop(columns=['date']).reset_index(drop=True)

            # Get and apply our model
            model = BinnedModel('tt_gain_model')
            xdf = model.fit(xdf, 'tt_followers', 'gain')

            # Remove anything that isn't at least +2std above mean and sort by z score
            # This is a slightly higher threshold because we have a much wider net because we have a lot more tiktok ids
            xdf = xdf[xdf['gain'] > xdf['mean'] + xdf['std'] * 2].sort_values(by='z-score', ascending=False).reset_index(drop=True)

            # Drop/rename columns
            xdf.drop(columns=['mean', 'std'], inplace=True)
            xdf.rename(columns={ 'z-score': 'tt_growth_score', 'gain': 'tt_followers_gained' }, inplace=True)

            # Add the metadata back onto the dataframe
            # Only keep the tiktok ids of the artist_id reference that has the most tw streams because that's likely the one that we want to match to
            metadata = df.sort_values(by=['tiktok_id', 'tw_streams'], ascending=False) \
                .drop_duplicates(subset=['tiktok_id'], keep='first') \
                .reset_index(drop=True)
            xdf = xdf.astype({ 'tiktok_id': 'str' }) # need to do this to match merging types
            xdf = pd.merge(xdf, metadata, on='tiktok_id', how='left')

            # Round these columns
            xdf['tt_followers'] = xdf['tt_followers'].astype(int)
            xdf['tt_followers_gained'] = xdf['tt_followers_gained'].astype(int)

            # Organize
            xdf = xdf[['artist_id', 'tiktok_id', 'artist', 'tt_followers', 'tt_followers_gained', 'tt_growth_score', 'tw_streams', 'lw_streams', 'pct_chg']]
        
            # Upload the data to our db
            # First match to the existing chart to find anything that's new
            string = """
                create temp table tmp_tt_follower_growth (
                    artist_id bigserial,
                    tiktok_id text,
                    artist text,
                    tt_followers int,
                    tt_followers_gained int,
                    tt_growth_score float,
                    tw_streams int,
                    lw_streams int,
                    pct_chg float
                );
            """
            self.db.execute(string)
            self.db.big_insert(xdf, 'tmp_tt_follower_growth')

            string = """
                create temp table tmp_data as (
                    select
                        t.artist_id,
                        t.tiktok_id,
                        t.artist,
                        t.tt_followers,
                        t.tt_followers_gained,
                        t.tt_growth_score,
                        t.tw_streams,
                        t.lw_streams,
                        t.pct_chg,
                        case
                            when g.artist_id is null then true
                            else false
                        end as is_new
                    from tmp_tt_follower_growth t
                    left join social_charts.tt_follower_growth g on t.artist_id = g.artist_id
                );

                delete from social_charts.tt_follower_growth;

                insert into social_charts.tt_follower_growth (artist_id, tiktok_id, artist, tt_followers, tt_followers_gained, tt_growth_score, tw_streams, lw_streams, pct_chg, is_new)
                select
                    artist_id,
                    tiktok_id,
                    artist,
                    tt_followers,
                    tt_followers_gained,
                    tt_growth_score,
                    tw_streams,
                    lw_streams,
                    pct_chg,
                    is_new
                from tmp_data;

                drop table tmp_data;
                drop table tmp_tt_follower_growth;
            """
            self.db.execute(string)
        
        string = """
            select
                m.artist_id,
                m.artist,
                m.tw_streams,
                coalesce(st.lw_streams, 0) as lw_streams,
                m.pct_chg,
                cm.youtube_id,
                cm.tiktok_id,
                cm.spotify_id,
                cm.instagram_id
            from nielsen_artist.__artist m
            left join nielsen_artist.cm_map cm on m.artist_id = cm.artist_id
            left join nielsen_artist.__stats st on m.artist_id = st.artist_id
            where signed is false
                and (
                    cm.youtube_id is not null or
                    cm.tiktok_id is not null or
                    cm.spotify_id is not null or
                    cm.instagram_id is not null
                )
        """
        df = self.db.execute(string)

        reporting_db = Db('reporting_db')
        reporting_db.connect()

        updateTiktokChart(df)
        updateSpotifyChart(df)
        updateInstagramChart(df)

        reporting_db.disconnect()
    
    def updateSpotifyCharts(self):

        def get_type(category):
        
            if category.startswith('regional') or category.startswith('viral'):
                return 'track'
            elif category.startswith('album'):
                return 'album'
            else:
                return 'artist'

        def package(entry, t):
            
            track = entry[t + 'Metadata']
            chart = entry['chartEntryData']
            
            if t == 'track' or t == 'album':
                title = track[t + 'Name']
                artist = '/'.join([artist['name'] for artist in track['artists']]) if 'artists' in track else ''
            else:
                artist = track[t + 'Name']
                title = ''

            spotify_image = track['displayImageUri'] if 'displayImageUri' in track else ''
            spotify_id = track[t + 'Uri'].split(':')[2] if t + 'Uri' in track else ''
            copyrights = '/'.join([label['name'] if 'name' in label else '' for label in track['labels']]) if 'labels' in track else ''

            appearances_on_chart = chart['appearancesOnChart']
            consecutive_appearances_on_chart = chart['consecutiveAppearancesOnChart']
            current_rank = chart['currentRank']
            entry_date = chart['entryDate']
            entry_rank = chart['entryRank']
            entry_status = chart['entryStatus']
            peak_date = chart['peakDate']
            peak_rank = chart['peakRank']
            previous_rank = chart['previousRank']
            streams = chart['rankingMetric']['value'] if 'rankingMetric' in chart and 'value' in chart['rankingMetric'] else 0
            
            return {
                'type': t,
                'title': title,
                'artist': artist,
                'copyrights': copyrights,
                'spotify_image': spotify_image,
                'spotify_id': spotify_id,
                'appearances_on_chart': appearances_on_chart,
                'consecutive_appearances_on_chart': consecutive_appearances_on_chart,
                'current_rank': current_rank,
                'entry_date': entry_date,
                'entry_rank': entry_rank,
                'entry_status': entry_status,
                'peak_date': peak_date,
                'peak_rank': peak_rank,
                'previous_rank': previous_rank,
                'streams': streams
            }
            
        def getFlagshipData(spotify, flagship_outline):

            flagship_data = []
            for flagship_item in flagship_outline:

                # Get data from url in this package
                url = flagship_item['url']
                res = spotify.get(url)

                if res is None:
                    continue

                data = []
                if 'entries' in res and len(res['entries']) > 0:

                    for entry in res['entries']:

                        if not ('missingRequiredFields' in entry and entry['missingRequiredFields'] is True):

                            t = get_type(flagship_item['category'])

                            data.append({
                                **package(entry, t),
                                **flagship_item
                            })

                flagship_data = [ *flagship_data, *data ]
                
            return pd.DataFrame(flagship_data)
            
        def getCityData(spotify, city_outline):

            city_data = []
            for city_item in city_outline:
                
                # Get data from url in this package
                url = city_item['url']
                res = spotify.get(url)

                if res is None:
                    continue
                
                data = []
                if 'entries' in res and len(res['entries']) > 0:
                    
                    for entry in res['entries']:
                        
                        if not ('missingRequiredFields' in entry and entry['missingRequiredFields'] is True):
                            data.append({
                                **package(entry, 'track'),
                                **city_item
                            })
                        
                city_data = [ *city_data, *data ]
                
            return pd.DataFrame(city_data)
            
        def getGenreData(spotify, genre_outline):

            genre_data = []
            for genre_item in genre_outline:
                
                url = genre_item['url']
                res = spotify.get(url)

                if res is None:
                    continue
                
                data = []
                if 'entries' in res and len(res['entries']) > 0:
                    
                    for entry in res['entries']:
                        
                        if not ('missingRequiredFields' in entry and entry['missingRequiredFields'] is True):
                        
                            data.append({
                                **package(entry, 'track'),
                                **genre_item
                            })
                        
                genre_data = [ *genre_data, *data ]
                
            return pd.DataFrame(genre_data)
            
        # Read in our outlines
        flagship_outline = self.db.execute('select * from misc.spotify_flagship_outline')
        city_outline = self.db.execute('select * from misc.spotify_city_outline')
        genre_outline = self.db.execute('select * from misc.spotify_genre_outline')

        if flagship_outline is None or city_outline is None or genre_outline is None:
            raise Exception('Error getting flagship, city or genre outlines')

        flagship_outline = flagship_outline.to_dict('records')
        city_outline = city_outline.to_dict('records')
        genre_outline = genre_outline.to_dict('records')

        # Init spotify client
        spotify = Spotify()

        # Get data from spotify's apis
        flagship_data = getFlagshipData(spotify, flagship_outline)
        city_data = getCityData(spotify, city_outline)
        genre_data = getGenreData(spotify, genre_outline)

        # Combine the datasets
        data = pd.concat([ flagship_data, city_data, genre_data ]).reset_index(drop=True)

        # Add the insert date to the data
        today = datetime.today().strftime('%Y-%m-%d')
        data['date'] = today

        # Sometimes we have duplicates in a chart, get rid of those
        data = data.drop_duplicates(subset=['spotify_id', 'name']).reset_index(drop=True)

        # Add a value that tells us how many times each track has popped up on any chart
        chart_count = data.value_counts(subset=['spotify_id']).rename('chart_count').reset_index()
        data = pd.merge(data, chart_count, on='spotify_id', how='left')

        # Now we need to attach to nielsen, extract the spotify ids and separate
        spotify_track_ids = tuple(data.loc[(~data['spotify_id'].isnull()) & (data['type'] == 'track'), 'spotify_id'].unique())
        spotify_artist_ids = tuple(data.loc[(~data['spotify_id'].isnull()) & (data['type'] == 'artist'), 'spotify_id'].unique())

        # Create temporary tables with track/artist ids
        string = """
            create temp table tmp_spotify_track_ids (id text);
            create temp table tmp_spotify_artist_ids (id text);
        """
        self.db.execute(string)
        self.db.big_insert(pd.DataFrame(spotify_track_ids, columns=['id']), 'tmp_spotify_track_ids')
        self.db.big_insert(pd.DataFrame(spotify_artist_ids, columns=['id']), 'tmp_spotify_artist_ids')

        # Use temporary tables to match on artist/song information
        string = """
            select
                artist_id as nielsen_id,
                spotify_artist_id as spotify_id,
                tw_streams,
                signed
            from nielsen_artist.__artist m
            join tmp_spotify_artist_ids x on x.id = m.spotify_artist_id
        """
        artists = self.db.execute(string)

        string = """
            select
                song_id as nielsen_id,
                spotify_track_id as spotify_id,
                tw_streams,
                signed
            from nielsen_song.__song m
            join tmp_spotify_track_ids x on x.id = m.spotify_track_id
        """
        songs = self.db.execute(string)

        # Cleanup
        string = """
            drop table tmp_spotify_track_ids;
            drop table tmp_spotify_artist_ids;
        """
        self.db.execute(string)

        if songs is None or artists is None:
            raise Exception('Error getting artists/songs for spotify chart updates')

        # Combine the artists / songs
        spotify_df = pd.concat([ songs, artists ]).reset_index(drop=True)

        # Merge with the spotify data we just gathered
        df = pd.merge(data, spotify_df, on='spotify_id', how='left')

        # Last we need to do a final check of signed/unsigned stuff
        df = self.basicSignedCheck(df)

        # Clean types
        df['nielsen_id'] = df['nielsen_id'].astype('Int64')
        df['tw_streams'] = df['tw_streams'].fillna(0).astype(int)

        # Remove existing data
        string = """
            delete from dsp_charts.spotify
        """
        self.db.execute(string)

        # Insert new data
        self.db.big_insert(df, 'dsp_charts.spotify')

        # Update the incrementor for number of charts things have been on
        string = """
            create temp table tmp_artists as (
                select
                    spotify_id,
                    chart_count
                from dsp_charts.spotify
                where type = 'artist'
                group by spotify_id, chart_count
                order by chart_count desc
                limit 200
            );

            delete
            from dsp_charts.spotify_artist_chart_count_tracker
            where spotify_id not in (select spotify_id from tmp_artists);

            insert into dsp_charts.spotify_artist_chart_count_tracker (spotify_id, consecutive_appearances_on_chart)
            select spotify_id, 0 from tmp_artists where spotify_id not in (select spotify_id from dsp_charts.spotify_artist_chart_count_tracker);

            update dsp_charts.spotify_artist_chart_count_tracker
            set consecutive_appearances_on_chart = consecutive_appearances_on_chart + 1;

            create temp table tmp_songs as (
                select
                    spotify_id,
                    chart_count
                from dsp_charts.spotify
                where type = 'track'
                group by spotify_id, chart_count
                order by chart_count desc
                limit 200
            );

            delete
            from dsp_charts.spotify_track_chart_count_tracker
            where spotify_id not in (select spotify_id from tmp_songs);

            insert into dsp_charts.spotify_track_chart_count_tracker (spotify_id, consecutive_appearances_on_chart)
            select spotify_id, 0 from tmp_songs where spotify_id not in (select spotify_id from dsp_charts.spotify_track_chart_count_tracker);

            update dsp_charts.spotify_track_chart_count_tracker
            set consecutive_appearances_on_chart = consecutive_appearances_on_chart + 1;

            create temp table tmp_albums as (
                select
                    spotify_id,
                    chart_count
                from dsp_charts.spotify
                where type = 'album'
                group by spotify_id, chart_count
                order by chart_count desc
                limit 200
            );

            delete
            from dsp_charts.spotify_album_chart_count_tracker
            where spotify_id not in (select spotify_id from tmp_albums);

            insert into dsp_charts.spotify_album_chart_count_tracker (spotify_id, consecutive_appearances_on_chart)
            select spotify_id, 0 from tmp_albums where spotify_id not in (select spotify_id from dsp_charts.spotify_album_chart_count_tracker);

            update dsp_charts.spotify_album_chart_count_tracker
            set consecutive_appearances_on_chart = consecutive_appearances_on_chart + 1;

            drop table tmp_artists;
            drop table tmp_songs;
            drop table tmp_albums;
        """
        self.db.execute(string)

    def testDbInsert(self):

        """
            Tests to make sure that we are able to insert something into our database.
        """

        # Rollback our changes
        # DO NOT TOUCH THIS LINE, WE CANNOT COMMIT CHANGES PRIOR TO THIS, BUT WE ARE GOING TO
        # COMMIT CHANGES IN THE NEXT LINES.
        self.db.rollback()

        # Create the test table if it doesn't already exist. I created it, but just in case
        # you want to delete it and retest...
        string = """
            create table if not exists pipeline_test (
                test_value text,
                timestamp timestamp default current_timestamp
            );
        """
        self.db.execute(string)

        # Get the number of rows in the pipeline_test table, which we will check against later
        # to make sure that something actually got inserted
        string = """
            select *
            from pipeline_test
        """
        df = self.db.execute(string)
        if df is None:
            raise Exception('Error getting pipeline_test table')
        num_rows_start = len(df)

        # Perform a test insert of this table. It comes with a timestamp so you should be able
        # to see the latest test case in there inserted if all goes according to plan.
        string = """
            insert into pipeline_test (test_value)
            values ('Pipeline test complete!')
        """
        self.db.execute(string)

        # Commit the test changes to the db
        self.db.commit()

        # Check rows after to make sure that the row was correctly inserted
        string = """
            select *
            from pipeline_test
        """
        df = self.db.execute(string)
        if df is None:
            raise Exception('Error getting pipeline_test table')
        num_rows_end = len(df)

        # If the number of rows isn't greater by 1 throw an error
        if num_rows_start + 1 != num_rows_end:
            raise Exception('Insert into pipeline_test did not get committed properly')
    
    def archiveNielsenFiles(self):

        s3_fullfile = US_S3_UPLOAD_FOLDER_TEMPLATE.format(self.files['zip'])
        self.aws.upload_s3(self.fullfiles['zip_local_archive'], s3_fullfile)

    def report_genius(self):

        """
            Generates the genius report, should be scheduled for every monday.
        """

        def get_genius_data(genre, page):

            url = f'https://genius.com/api/songs/chart?time_period=day&chart_genre={genre}&page={page}&per_page=50'
            res = requests.get(url)
            res = res.json()
            
            items = res['response']['chart_items']
            
            return items

        def unwrap_item(item, rank, genre):
            
            genius_id = item['item']['id']
            stats = item['item']['stats']
            concurrents = stats['concurrents'] if 'concurrents' in stats else 0
            is_hot = stats['hot'] if 'hot' in stats else 0
            views = stats['pageviews'] if 'pageviews' in stats else 0
            title = unicodedata.normalize('NFKD', item['item']['full_title'])
            artist = unicodedata.normalize('NFKD', item['item']['artist_names'])
            song_rank = next(rank)
            
            return {
                'genius_id': genius_id,
                'concurrents': concurrents,
                'is_hot': is_hot,
                'rank': song_rank,
                'views': views,
                'title': title,
                'artist': artist,
                'genre': genre
            }

        def rank_gen():
            n = 1
            while True:
                yield n
                n += 1

        # Start by getting all the data and aggregating together
        genres = ['all', 'pop', 'rap', 'rb', 'rock', 'country']
        items = []
        for genre in genres:
            
            idx = 1
            rank = rank_gen()
            while True:

                data = get_genius_data(genre, idx)

                if len(data) == 0:
                    break

                items += [unwrap_item(item, rank, genre) for item in data]

                idx += 1

        df = pd.DataFrame(items)

        # Clean types
        df = df.astype({
            'genius_id': 'str',
            'concurrents': 'int',
            'is_hot': 'bool',
            'views': 'int',
            'title': 'str',
            'artist': 'str',
            'genre': 'str'
        })

        reporting_db = Db('reporting_db')
        reporting_db.connect()

        # Get an isrcs from the reporting db that exist in conjunction with the genius ids
        string = """
            select id as genius_id, isrc
            from chartmetric_raw.genius_track
            where id in %(genius_ids)s
                and isrc is not null
        """
        params = { 'genius_ids': tuple(df['genius_id'].unique()) }
        ids = reporting_db.execute(string, params)
        ids = ids.astype({ 'genius_id': 'str' })

        reporting_db.disconnect()

        df = pd.merge(df, ids, on='genius_id', how='left')

        existing_isrcs = self.getExistingSpotifySongInfoByIsrc(df.isrc.values)

        # Add the spotify and extra data to genius data
        df = pd.merge(df, existing_isrcs, on='isrc', how='left')

        # Perform one last scan for signed artists based on the spotify copyright info
        df = self.basicSignedCheck(df)

        # Clean up the dataframe
        keep_cols = [
            'genius_id',
            'song_id',
            'title',
            'artist',
            'concurrents',
            'is_hot',
            'rank',
            'views',
            'signed',
            'copyrights',
            'tw_streams',
            'lw_streams',
            'pct_chg'
        ]
        df = df[keep_cols].reset_index(drop=True)

        # Add a date column
        t = today()
        df['date'] = t

        # Write export to reports folder
        df.to_csv(self.reports_fullfiles['genius_scrape'], index=False)

    def report_dailySongs(self):

        # Convert columns into correct date indicies
        def getDateIndicies(cols):
            idx = []
            for col in cols:
                d = str2Date(col)
                if d:
                    idx.append(col)
            return idx

        # Convert a string to a date object
        def str2Date(s):
            try:
                return datetime.strptime(s, date_format())
            except ValueError:
                try:
                    return datetime.strptime(s, '%d/%m/%Y')
                except ValueError:
                    return False
                
        def date_format():
            return '%m/%d/%Y'

        # Clean and standardize data/columns
        def cleanColumns(df):

            # We can remove premium/oda columns because they aren't relevant in this context
            df = df[df.columns.drop(list(df.filter(regex='Ad Supported ODA')))]
            df = df[df.columns.drop(list(df.filter(regex='Premium ODA')))]

            # All that's left for the daily streaming is the total ODA and we can just remove that
            # part of the substring to make it easier to deal with dates in the columns
            df = df.rename(columns={ i: i.replace(' - Total ODA', '') for i in df.columns })
            
            # Rename the remaining columns for consistency and database usage
            renameable = {
                'TW Rank': 'tw_rank',
                'LW Rank': 'lw_rank',
                'Artist': 'artist',
                'Title': 'title',
                'Unified Song Id': 'unified_song_id',
                'LW On-Demand Audio Streams': 'lw_oda_streams',
                'L2W_On_Demand_Audio_Streams': 'l2w_oda_streams',
                'Weekly %change On-Demand Audio Streams': 'weekly_pct_chg_oda_streams',
                'YTD On-Demand Audio Streams': 'ytd_oda_streams',
                'Top ISRC': 'isrc',
                'Label Abbrev': 'label',
                'CoreGenre': 'core_genre',
                'Release_date': 'release_date',
                'WTD Building ODA (Friday-Thursday)': 'wtd_building_oda',
                'TW On-Demand Audio Streams': 'tw_oda_streams',
                'RTD On-Demand Audio Streams': 'rtd_oda_streams',
                '7-day Rolling ODA': 'tw_rolling_oda',
                'pre-7days rolling ODA': 'lw_rolling_oda',
                'TW Digital Track Sales': 'tw_digital_track_sales',
                'ATD Digital Track Sales': 'atd_digital_track_sales',
                'YTD Digital Track Sales': 'ytd_digital_track_sales',
                'TW On-Demand Video': 'tw_odv',
                'LW On-Demand Video': 'lw_odv',
                'YTD On-Demand Video': 'ytd_odv',
                'ATD On-Demand Video': 'atd_odv'
            }
            
            df = df.rename(columns=renameable)
            
            # Clean the types
            df = df.astype({
                'tw_rank': 'int',
                'lw_rank': 'int',
                'artist': 'str',
                'title': 'str',
                'unified_song_id': 'str',
                'label': 'str',
                'core_genre': 'str',
                'tw_oda_streams': 'int',
                'lw_oda_streams': 'int',
                'l2w_oda_streams': 'int',
                'ytd_oda_streams': 'int',
                'rtd_oda_streams': 'int',
                'wtd_building_oda': 'float',
                'tw_rolling_oda': 'int',
                'lw_rolling_oda': 'int',
                'tw_digital_track_sales': 'int',
                'ytd_digital_track_sales': 'int',
                'atd_digital_track_sales': 'int',
                'tw_odv': 'int',
                'lw_odv': 'int',
                'ytd_odv': 'int',
                'atd_odv': 'int'
            })
            
            # Remove the .0 from unified_song_id just in case
            df = df.astype({ 'unified_song_id': 'str' })
            df['unified_song_id'] = df.apply(lambda x: x.unified_song_id.split('.')[0], axis=1)
            
            return df

        df = pd.read_csv(self.fullfiles['song'], encoding='UTF-16')

        # Clean columns
        df = cleanColumns(df)

        # Get date indicies
        dateCols = getDateIndicies(df.columns)

        # Remove anything that hasn't done >10k streams 3 days ago
        df = df[df[dateCols[1]] >= 10000].reset_index(drop=True)

        # Add some spotify info and filter out signed acts
        string = """
            create temp table unified_song_ids (
                unified_song_id text
            );
        """
        self.db.execute(string)
        self.db.big_insert(df[['unified_song_id']], 'unified_song_ids')

        string = """
            select
                m.unified_song_id,
                m.signed,
                asp.genres,
                sp.copyrights,
                sp.instrumentalness
            from nielsen_song.__song m
            join unified_song_ids u on m.unified_song_id = u.unified_song_id
            left join nielsen_artist.spotify asp on asp.artist_id = m.artist_id
            left join nielsen_song.spotify sp on m.song_id = sp.song_id
        """
        signed_df = self.db.execute(string)
        df = pd.merge(df, signed_df, on='unified_song_id', how='left')
        df = df[df['signed'] != True].reset_index(drop=True)

        # Delete the temp table just to be neat in the cleanup
        string = 'drop table unified_song_ids'
        self.db.execute(string)

        # Add a bunch of extra stats

        # Percent change lw -> tw
        df['pct_chg'] = df['tw_oda_streams'].div(df['lw_oda_streams']) - 1

        # Rolling percent change lw -> tw
        df['rolling_pct_chg'] = df['tw_rolling_oda'].div(df['lw_rolling_oda']) - 1

        # 2 Day percent change
        df['2_day_chg'] = df[dateCols[0]].div(df[dateCols[1]]) - 1

        # 3 Day percent change
        df['3_day_chg'] = df[dateCols[1]].div(df[dateCols[2]]) - 1

        # 7 Day Acceleration
        df['7_day_acc'] = df[dateCols[1]].div(df[dateCols[7]]).pow(1 / 6) - 1

        # 4 Day Acceleration
        df['4_day_acc'] = df[dateCols[1]].div(df[dateCols[4]]).pow(1 / 3) - 1

        # Filter based on aaron's criteria
        mask = (
            (df['rolling_pct_chg'] >= 0) & \
            (
                (
                    (df['2_day_chg'] >= 0.15) & \
                    (df['3_day_chg'] > 0)
                ) |
                (df['3_day_chg'] >= 0.25) |
                (
                    (df['7_day_acc'] >= 0.15) & \
                    (df['4_day_acc'] > 0)
                ) |
                (
                    (df['rolling_pct_chg'] >= 0.5) & \
                    (df['lw_rolling_oda'] > 100000)
                ) |
                (
                    (df['4_day_acc'] >= 0.15) & \
                    (df['3_day_chg'] > 0)
                )
            )
        )

        df = df[mask].reset_index(drop=True)

        string = """
            create temp table unified_song_ids (
                unified_song_id text
            );
        """
        self.db.execute(string)
        self.db.big_insert(df[['unified_song_id']], 'unified_song_ids')

        string = """
            select
                u.unified_song_id,
                s.date,
                s.streams
            from unified_song_ids u
            left join nielsen_song.meta m on u.unified_song_id = m.unified_song_id
            left join nielsen_song.streams s on m.id = s.song_id
            where date < ( select value::date + interval '1 day' from nielsen_meta where id = 1 )
                and date > ( select value::date - interval '14 days' from nielsen_meta where id = 1 )
        """
        streams = self.db.execute(string)

        # Delete the temp table just to be neat in the cleanup
        string = 'drop table unified_song_ids'
        self.db.execute(string)

        # Pivot the table so that each row is a song and each column is a date
        streams = streams.pivot(index='unified_song_id', columns='date', values='streams')
        streams.columns.name = None
        streams = streams.reset_index()

        # Columns need to be strings instead of datetime
        streams.columns = streams.columns.astype('str')

        def getDateColumnsInOrder(cols):
            """
            
                Function to get the date columns as an array ordered by date
                from most recent to least recent.
            
            """
                
            cols = cols[1:]
            cols = sorted(cols, key=lambda x: datetime.strptime(x, '%Y-%m-%d'))
            cols = cols[::-1]
            
            return cols

        cols = getDateColumnsInOrder(streams.columns)

        # Merge the streaming data onto the main df
        df = pd.merge(df, streams, on='unified_song_id', how='left')

        # Reformat for aaron
        df.rename(columns={
            'unified_song_id': 'Unifiedsongid',
            'release_date': 'Release_date',
            'copyrights': 'copyright',
            'label': 'Label',
            'core_genre': 'CoreGenre',
            'genres': 'artist_genres',
            'tw_oda_streams': 'TW On-Demand Audio Streams',
            'lw_oda_streams': 'LW On-Demand Audio Streams',
            'l2w_oda_streams': 'L2W_On_Demand_Audio_Streams',
            'ytd_oda_streams': 'YTD On-Demand Audio Streams',
            'tw_rolling_oda': 'TW Rolling ODA',
            'rolling_pct_chg': 'rolling_pct_chg',
            'lw_rolling_oda': 'LW Rolling ODA'
        }, inplace=True)

        df = df[[
            'Unifiedsongid', 'artist', 'title', 'copyright', 'Label', 'CoreGenre',
            'Release_date', 'artist_genres', 'instrumentalness', 'TW On-Demand Audio Streams', 'pct_chg', 'LW On-Demand Audio Streams',
            'L2W_On_Demand_Audio_Streams', 'YTD On-Demand Audio Streams', 'TW Rolling ODA', 'rolling_pct_chg', 'LW Rolling ODA', '7_day_acc',
            '4_day_acc', cols[0], '2_day_chg', cols[1], '3_day_chg', cols[2], cols[3],
            cols[4], cols[5], cols[6], cols[7], cols[8], cols[9], cols[10], cols[11], cols[12], cols[13]
        ]].reset_index(drop=True)

        df.to_csv(self.reports_fullfiles['nielsen_daily_audio'], index=False)

    def getExistingSpotifySongInfoByIsrc(self, isrcs):

        """
            @param isrcs | array[str | None]

            Basically all you need to do is gather all the isrcs you want to match and pass them as an array
            to this function. It will drop duplicates, get rid of empty strings & None values. Once it has cleaned
            it up, it will create a temporary table in our db, match both our nielsen_song.spotify and nielsen_song.spotify_extra
            tables against the isrcs you passed. The temp table will be dropped and it will return a dataframe with the following columns...

            @returns df('song_id', 'isrc', 'signed', 'copyrights', 'tw_streams', 'lw_streams', 'pct_chg')
        """

        # Convert the array to a dataframe for easy data cleanup handling
        isrcs = pd.DataFrame({ 'isrc': isrcs })

        # Drop any null values, empty strings & duplicates
        isrcs = isrcs[(~isrcs['isrc'].isnull()) & (isrcs['isrc'] != '')].drop_duplicates(subset=['isrc']).reset_index(drop=True)

        # Create temp table for merge matching, we know this technique...
        string = """
            create temp table tmp_isrcs (
                isrc text
            );
        """
        self.db.execute(string)
        self.db.big_insert(isrcs, 'tmp_isrcs')

        # Select the data from our tables using merge matching
        # Select data from both nielsen_song.spotify & nielsen_song.spotify_extra
        string = """
            with sp as (
                select
                    sp.song_id,
                    sp.isrc,
                    coalesce(rr.signed, false) as signed,
                    sp.copyrights,
                    st.tw_streams,
                    st.lw_streams,
                    st.pct_chg
                from tmp_isrcs ti
                join nielsen_song.spotify sp on ti.isrc = sp.isrc
                left join nielsen_song.__reports_recent rr on sp.song_id = rr.song_id
                left join nielsen_song.__stats st on sp.song_id = st.song_id
            ), unfound_isrcs as (
                select isrc
                from (
                    select
                        ti.isrc,
                        case
                            when sp.isrc is null then false
                            else true
                        end as is_found
                    from tmp_isrcs ti
                    left join sp on ti.isrc = sp.isrc
                ) q
                where is_found is false
            ), sp_extra as (
                select
                    null::bigint as song_id,
                    spe.isrc,
                    false as signed,
                    copyrights,
                    null::bigint as tw_streams,
                    null::bigint as lw_streams,
                    null::float as pct_chg
                from unfound_isrcs ufi
                join nielsen_song.spotify_extra spe on ufi.isrc = spe.isrc
            )

            select * from sp
            union all
            select * from sp_extra
        """
        existing_isrcs = self.db.execute(string)

        # Drop out temporary table to stay clean
        string = 'drop table tmp_isrcs'
        self.db.execute(string)

        # Lastly, we can drop duplicate rows
        # But first we want to sort it by tw_streams, because we may have found 2 songs with the same
        # isrcs, and we want to keep the one with higher streaming volume because that's likely the one
        # we're looking for
        existing_isrcs = existing_isrcs.sort_values(by='tw_streams', ascending=False).reset_index(drop=True)
        existing_isrcs = existing_isrcs.drop_duplicates(subset=['isrc'], keep='first').reset_index(drop=True)

        return existing_isrcs
    
    def getExistingSpotifyArtistInfoBySpotifyArtistId(self, spotify_artist_ids):

        """
            @param spotify_artist_ids | array[str | None]

            Pass an array of spotify artist ids and this function will do its best to match them
            to the existing cached spotify data in our database. It will return you a dataframe of
            that data.

            @returns df(spotify_artist_id, artist_id, signed, tw_streams, lw_streams, pct_chg, copyrights)
        """

        # Convert the array to a dataframe for easy data cleanup and handling
        spotify_artist_ids = pd.DataFrame({ 'spotify_artist_id': spotify_artist_ids })

        # Drop any null values, empty strings & duplicates
        m1 = ~spotify_artist_ids['spotify_artist_id'].isnull()
        m2 = spotify_artist_ids['spotify_artist_id'] != ''
        spotify_artist_ids = spotify_artist_ids[m1 & m2]
        spotify_artist_ids = spotify_artist_ids.drop_duplicates(subset=['spotify_artist_id']).reset_index(drop=True)

        # Create temp table for merge matching, we know this technique...
        string = """
            create temp table tmp_spotify_artist_ids (
                spotify_artist_id text
            );
        """
        self.db.execute(string)
        self.db.big_insert(spotify_artist_ids, 'tmp_spotify_artist_ids')

        string = """
            select
                sp.spotify_artist_id,
                m.artist_id,
                coalesce(m.signed, false) as signed,
                coalesce(st.tw_streams, 0) as tw_streams,
                coalesce(st.lw_streams, 0) as lw_streams,
                coalesce(st.pct_chg, 0) as pct_chg,
                sp.spotify_copyrights as copyrights
            from tmp_spotify_artist_ids tsai
            join nielsen_artist.spotify sp on tsai.spotify_artist_id = sp.spotify_artist_id
            left join nielsen_artist.__artist m on sp.artist_id = m.artist_id
            left join nielsen_artist.__stats st on sp.artist_id = st.artist_id
        """
        existing = self.db.execute(string)

        # Drop temporary table to stay clean
        string = 'drop table tmp_spotify_artist_ids'
        self.db.execute(string)

        # Drop duplicate rows, keep the spotify artist id that has the most tw streams associated with it
        existing = existing.sort_values(by='tw_streams', ascending=False).reset_index(drop=True)
        existing = existing.drop_duplicates(subset=['spotify_artist_id'], keep='first').reset_index(drop=True)

        return existing
    
    def report_shazam(self):

        reporting_db = Db('reporting_db')
        reporting_db.connect()

        def generateShazamRankByCountry():

            # Pull today's Shazam data
            string = """
                with tp as (
                    select
                        itunes.isrc,
                        shazam_track_id as shazam_id,
                        itunes.artist_name as artist,
                        itunes.track_name as title,
                        rank as tp_rank
                    from chartmetric_raw.shazam_chart
                    inner join chartmetric_raw.shazam on shazam.id = shazam_chart.shazam_track_id
                    inner join chartmetric_raw.itunes on itunes.itunes_track_id = shazam.itunes_track_id
                    where country = 'US'
                        and timestp = dateadd('days', -2, current_date)
                        and city is null
                ), lp as (
                    select
                        itunes.isrc,
                        rank as lp_rank
                    from chartmetric_raw.shazam_chart
                    inner join chartmetric_raw.shazam on shazam.id = shazam_chart.shazam_track_id
                    inner join chartmetric_raw.itunes on itunes.itunes_track_id = shazam.itunes_track_id
                    where country = 'US'
                        and timestp = dateadd('days', -3, current_date)
                        and city is null
                ), lw as (
                    select
                        itunes.isrc,
                        rank as lw_rank
                    from chartmetric_raw.shazam_chart
                    inner join chartmetric_raw.shazam on shazam.id = shazam_chart.shazam_track_id
                    inner join chartmetric_raw.itunes on itunes.itunes_track_id = shazam.itunes_track_id
                    where country = 'US'
                        and timestp = dateadd('days', -7, current_date)
                        and city is null
                )

                select tp.*, lp.lp_rank, lw.lw_rank
                from tp
                left join lp on tp.isrc = lp.isrc
                left join lw on tp.isrc = lw.isrc
            """
            shazam = reporting_db.execute(string)

            existing_isrcs = self.getExistingSpotifySongInfoByIsrc(shazam.isrc.values)

            # Get all the rows that do not have existing data in our database
            new_items = shazam[~shazam['isrc'].isin(existing_isrcs.isrc)].drop_duplicates(subset=['isrc']).reset_index(drop=True)
            new_items = new_items[['isrc', 'artist', 'title']]

            # Get their track level data from spotify
            new_items = getSpotifyTrackDataFromSpotifyUsingIsrcTitleAndArtist(new_items)

            # We're going to cache those new items now, so we need to drop the artist/title columns and
            # drop anything that didn't result with an isrc
            new_items = new_items.drop(columns=['artist', 'title'])
            new_items = new_items[~new_items['isrc'].isnull()].reset_index(drop=True)

            # Only insert if we have something to insert
            if not new_items.empty:
                self.db.big_insert(new_items, 'nielsen_song.spotify_extra')

            # This can happen if we don't actually have anything new, so just add the column if that's the case
            if 'copyrights' not in new_items:
                new_items['copyrights'] = None

            # Alter our new items to match the shape of our existing items
            new_items = new_items[['isrc', 'copyrights']]
            new_items[['signed', 'tw_streams', 'lw_streams', 'pct_chg']] = None

            # Concat together our existing and new items
            keep_columns = ['isrc', 'copyrights', 'signed', 'tw_streams', 'lw_streams', 'pct_chg']
            spotify_df = pd.concat([existing_isrcs[keep_columns], new_items[keep_columns]]).reset_index(drop=True)

            # Now that we have, to the best of our ability, all the spotify information in one dataframe, we can
            # merge that onto our shazam data using the isrc as the key
            shazam = pd.merge(shazam, spotify_df, on='isrc', how='left')

            # Any place where the signed data is None, just assume that that's false
            shazam.loc[shazam['signed'].isnull(), 'signed'] = False

            # We're going to double check what is and isn't signed
            # First we'll create a meta column on unique isrc so we don't duplicate
            # We also don't need to check for songs that are already marked as signed
            # Last, let's just drop any unnecessary columns for cleanlyness
            meta = shazam[shazam['signed'] == False].drop_duplicates(subset=['isrc']).reset_index(drop=True)
            meta = meta[['isrc', 'artist', 'copyrights', 'signed']]

            meta = self.basicSignedCheck(meta)

            # First remove the isrcs we have already identified as signed
            shazam = shazam[shazam['signed'] != True].reset_index(drop=True)

            # Swap the 'signed' column on the shazam table with the meta table so we can filter out
            # the signed artists that we just found
            shazam = shazam.drop(columns=['signed'])
            shazam = pd.merge(shazam, meta[['isrc', 'signed']], on='isrc', how='left')
            shazam = shazam[shazam['signed'] != True].reset_index(drop=True)

            # We're going to do some math on these shazam rank columns so we can fill the null values with 0s
            math_cols = ['tp_rank', 'lp_rank', 'lw_rank', 'tw_streams', 'lw_streams', 'pct_chg']
            shazam[math_cols] = shazam[math_cols].fillna(0)

            # Also just fix the types to be sure
            math_cols = { col: 'int' for col in math_cols }
            shazam = shazam.astype(math_cols)

            # Introduce new stats columns we're going to calculate
            stat_cols = [
                'rank_2_day_chg',
                'rank_7_day_chg'
            ]
            shazam[stat_cols] = None

            # Add rank changes, if something is 0 in the comparison day then it's new
            shazam.loc[shazam['lp_rank'] == 0, 'rank_2_day_chg'] = 'New'
            shazam.loc[shazam['lp_rank'] != 0, 'rank_2_day_chg'] = shazam['lp_rank'] - shazam['tp_rank']

            shazam.loc[shazam['lw_rank'] == 0, 'rank_7_day_chg'] = 'New'
            shazam.loc[shazam['lw_rank'] != 0, 'rank_7_day_chg'] = shazam['lw_rank'] - shazam['tp_rank']

            return shazam

        def generateShazamRankByCity(shazam_rank_by_country):

            string = """
                with tp as (
                    select
                        itunes.isrc,
                        shazam_track_id as shazam_id,
                        itunes.artist_name as artist,
                        itunes.track_name as title,
                        city as city_name,
                        rank as tp_rank
                    from chartmetric_raw.shazam_chart
                    inner join chartmetric_raw.shazam on shazam.id = shazam_chart.shazam_track_id
                    inner join chartmetric_raw.itunes on itunes.itunes_track_id = shazam.itunes_track_id
                    where country = 'US'
                        and timestp = dateadd('days', -2, current_date)
                ), lp as (
                    select
                        itunes.isrc,
                        city as city_name,
                        rank as lp_rank
                    from chartmetric_raw.shazam_chart
                    inner join chartmetric_raw.shazam on shazam.id = shazam_chart.shazam_track_id
                    inner join chartmetric_raw.itunes on itunes.itunes_track_id = shazam.itunes_track_id
                    where country = 'US'
                        and timestp = dateadd('days', -3, current_date)
                ), lw as (
                    select
                        itunes.isrc,
                        city as city_name,
                        rank as lw_rank
                    from chartmetric_raw.shazam_chart
                    inner join chartmetric_raw.shazam on shazam.id = shazam_chart.shazam_track_id
                    inner join chartmetric_raw.itunes on itunes.itunes_track_id = shazam.itunes_track_id
                    where country = 'US'
                        and timestp = dateadd('days', -7, current_date)
                )

                select tp.*, lp.lp_rank, lw.lw_rank
                from tp
                left join lp on tp.isrc = lp.isrc and tp.city_name = lp.city_name
                left join lw on tp.isrc = lw.isrc and tp.city_name = lw.city_name
            """
            shazam = reporting_db.execute(string)

            existing_isrcs = self.getExistingSpotifySongInfoByIsrc(shazam.isrc.values)

            # Get all the rows that do not have existing data in our database
            new_items = shazam[~shazam['isrc'].isin(existing_isrcs.isrc)].drop_duplicates(subset=['isrc']).reset_index(drop=True)
            new_items = new_items[['isrc', 'artist', 'title']]

            # Get their track level data from spotify
            new_items = getSpotifyTrackDataFromSpotifyUsingIsrcTitleAndArtist(new_items)

            # We're going to cache those new items now, so we need to drop the artist/title columns and
            # drop anything that didn't result with an isrc
            new_items = new_items.drop(columns=['artist', 'title'])
            new_items = new_items[~new_items['isrc'].isnull()].reset_index(drop=True)

            # Only insert if we have something to insert
            if not new_items.empty:
                self.db.big_insert(new_items, 'nielsen_song.spotify_extra')

            # This can happen if we don't actually have anything new, so just add the column if that's the case
            if 'copyrights' not in new_items:
                new_items['copyrights'] = None

            # Alter our new items to match the shape of our existing items
            new_items = new_items[['isrc', 'copyrights']].reset_index(drop=True)
            new_items[['signed', 'tw_streams', 'lw_streams', 'pct_chg']] = None

            # Concat together our existing and new items
            keep_columns = ['isrc', 'copyrights', 'signed', 'tw_streams', 'lw_streams', 'pct_chg']
            spotify_df = pd.concat([existing_isrcs[keep_columns], new_items[keep_columns]]).reset_index(drop=True)

            # Now that we have, to the best of our ability, all the spotify information in one dataframe, we can
            # merge that onto our shazam data using the isrc as the key
            shazam = pd.merge(shazam, spotify_df, on='isrc', how='left')

            # Add the market ranks
            market_rank = self.db.execute('select * from misc.market_rank')
            shazam = pd.merge(shazam, market_rank[['city_name', 'market_rank']], how='left', on='city_name')
            national_isrcs = shazam_rank_by_country.isrc.unique()

            # Add a column that marks whether it landed on the national chart or not
            shazam['national_chart'] = False
            shazam.loc[shazam['isrc'].isin(national_isrcs), 'national_chart'] = True

            # Add stats columns
            stats_cols = [
                'rank_2_day_chg',
                'rank_7_day_chg'
            ]
            shazam[stats_cols] = None

            # We're going to do some math on these shazam rank columns so we can fill the null values with 0s
            math_cols = ['tp_rank', 'lp_rank', 'lw_rank', 'tw_streams', 'lw_streams', 'pct_chg']
            shazam[math_cols] = shazam[math_cols].fillna(0)

            # Also just fix the types to be sure
            math_cols = { col: 'int' for col in math_cols }
            shazam = shazam.astype(math_cols)

            # Add rank changes, if something is 0 in the comparison day then it's new
            shazam.loc[shazam['lp_rank'] == 0, 'rank_2_day_chg'] = 'New'
            shazam.loc[shazam['lp_rank'] != 0, 'rank_2_day_chg'] = shazam['lp_rank'] - shazam['tp_rank']

            shazam.loc[shazam['lw_rank'] == 0, 'rank_7_day_chg'] = 'New'
            shazam.loc[shazam['lw_rank'] != 0, 'rank_7_day_chg'] = shazam['lw_rank'] - shazam['tp_rank']

            # Drop duplicate shazam ids within each city
            shazam = shazam.drop_duplicates(subset=['shazam_id', 'city_name']).reset_index(drop=True)

            # Do one more check for signed things
            shazam = self.basicSignedCheck(shazam)

            meta = shazam.drop_duplicates(subset=['shazam_id']).reset_index(drop=True)
            counts = shazam.groupby('shazam_id').size().rename('count').reset_index()
            meta = pd.merge(meta, counts, on='shazam_id')

            # We don't need the market rank on the meta report
            meta = meta.drop(columns='market_rank')

            return meta, shazam
        
        shazam_rank_by_country = generateShazamRankByCountry()
        shazam_by_market_streams, shazam_by_market = generateShazamRankByCity(shazam_rank_by_country)

        shazam_rank_by_country.to_csv(self.reports_fullfiles['shazam_rank_by_country'], index=False)
        shazam_by_market_streams.to_csv(self.reports_fullfiles['shazam_by_market_streams'], index=False)
        shazam_by_market.to_csv(self.reports_fullfiles['shazam_by_market'], index=False)

        reporting_db.disconnect()

    def report_spotifyArtistStatGrowth(self):

        reporting_db = Db('reporting_db')
        reporting_db.connect()

        string = """
            with tw as (
                select
                    spotify_artist_id,
                    artist_name,
                    genres,
                    monthly_listeners as tw_monthly_listeners,
                    popularity as tw_popularity,
                    followers as tw_followers
                from chartmetric_raw.spotify_artist_stat
                inner join chartmetric_raw.spotify_artist on spotify_artist_stat.spotify_artist = spotify_artist.id
                where monthly_listeners is not null
                    and timestp = dateadd('days', -3, current_date)
            ), lw as (
                select
                    spotify_artist_id,
                    monthly_listeners as lw_monthly_listeners,
                    popularity as lw_popularity,
                    followers as lw_followers
                from chartmetric_raw.spotify_artist_stat
                inner join chartmetric_raw.spotify_artist on spotify_artist_stat.spotify_artist = spotify_artist.id
                where monthly_listeners is not null
                    and timestp = dateadd('days', -10, current_date)
            )

            select
                tw.*,
                lw.lw_monthly_listeners,
                lw.lw_popularity,
                lw.lw_followers
            from tw
            left join lw on tw.spotify_artist_id = lw.spotify_artist_id
            where tw.tw_monthly_listeners >= 100000
                and lw.lw_monthly_listeners != 0
        """
        df = reporting_db.execute(string)

        reporting_db.disconnect()

        # Do some stats calculations
        df['montly_listeners_pct_chg'] = ((df['tw_monthly_listeners']).div(df['lw_monthly_listeners']) - 1) * 100
        df['popularity_pct_chg'] = ((df['tw_popularity']).div(df['lw_popularity']) - 1) * 100
        df['followers_pct_chg'] = ((df['tw_followers']).div(df['lw_followers']) - 1) * 100

        # Filter out anything that doesn't have at least a >15% increase in one of the stats
        mask = (
            (df['montly_listeners_pct_chg'] > 15) | \
            (df['popularity_pct_chg'] > 15) | \
            (df['followers_pct_chg'] > 15)
        )

        df = df[mask].reset_index(drop=True)

        # Attach the spotify data that already exists in our database
        spotify_df = self.getExistingSpotifyArtistInfoBySpotifyArtistId(df.spotify_artist_id.values)

        # Add the extra info
        df = pd.merge(df, spotify_df, on='spotify_artist_id', how='left')

        # Drop anything that's signed
        df = df[df['signed'] != True].reset_index(drop=True)

        df.to_csv(self.reports_fullfiles['spotify_artist_stat_growth'], index=False)
    
    def report_artist8WeekGrowth(self):

        string = """
            select
                q.artist_id,
                m.artist,
                m.track_count,
                q.num_positive_weeks,
                m.pct_chg,
                m.rtd_oda_streams,
                m.tw_streams,
                m.genres
            from (
                select
                    artist_id,
                    sum(is_positive) as num_positive_weeks,
                    sum(pct_chg) as rolling_pct_chg
                from (
                    select
                        artist_id,
                        case
                            when lag_streams is null or lag_streams = 0 then 0
                            when round(100 * (streams - lag_streams)::numeric / lag_streams, 2) < -50 then -50
                            when round(100 * (streams - lag_streams)::numeric / lag_streams, 2) > 50 then 50
                            else round(100 * (streams - lag_streams)::numeric / lag_streams, 2)
                        end as pct_chg,
                        case
                            when streams > lag_streams then 1
                            else 0
                        end as is_positive
                    from (
                        select *, lag(streams, 1) over (partition by artist_id order by weekly) as lag_streams
                        from (
                            select
                                artist_id,
                                date_trunc('week', date) as weekly,
                                sum(streams) as streams
                            from (
                                select
                                    m.artist_id,
                                    date - ( select extract(dow from value::date - interval '1 day')::int as number_of_days from nielsen_meta where id = 1 ) as date,
                                    streams
                                from ( select artist_id from nielsen_artist.__artist where tw_streams > 100000 and signed = false ) m
                                left join nielsen_artist.streams s on m.artist_id = s.artist_id
                                where date > ( select value::date from nielsen_meta where id = 1 ) - interval '64 days'
                                    and date < ( select value::date from nielsen_meta where id = 1 )
                                order by date
                            ) q
                            group by artist_id, weekly
                        ) q
                    ) q
                ) q
                group by artist_id
            ) q
            left join nielsen_artist.__artist m on q.artist_id = m.artist_id
            where rolling_pct_chg > 0
                and num_positive_weeks > 4
            order by num_positive_weeks desc, rolling_pct_chg desc
        """
        df = self.db.execute(string)

        df.to_csv(self.reports_fullfiles['artist_8_week_growth'], index=False)
    
    def report_song8WeekGrowth(self):

        string = """
            select
                q.song_id,
                m.artist,
                m.title,
                q.num_positive_weeks,
                m.pct_chg,
                m.rtd_oda_streams,
                m.tw_streams
            from (
                select
                    song_id,
                    sum(is_positive) as num_positive_weeks,
                    sum(pct_chg) as rolling_pct_chg
                from (
                    select
                        song_id,
                        case
                            when lag_streams is null or lag_streams = 0 then 0
                            when round(100 * (streams - lag_streams)::numeric / lag_streams, 2) < -50 then -50
                            when round(100 * (streams - lag_streams)::numeric / lag_streams, 2) > 50 then 50
                            else round(100 * (streams - lag_streams)::numeric / lag_streams, 2)
                        end as pct_chg,
                        case
                            when streams > lag_streams then 1
                            else 0
                        end as is_positive
                    from (
                        select *, lag(streams, 1) over (partition by song_id order by weekly) as lag_streams
                        from (
                            select
                                song_id,
                                date_trunc('week', date) as weekly,
                                sum(streams) as streams
                            from (
                                select
                                    m.song_id,
                                    date - ( select extract(dow from value::date - interval '1 day')::int as number_of_days from nielsen_meta where id = 1 ) as date,
                                    streams
                                from ( select song_id from nielsen_song.__song where tw_streams > 100000 and signed = false ) m
                                left join nielsen_song.streams s on m.song_id = s.song_id
                                where date > ( select value::date from nielsen_meta where id = 1 ) - interval '64 days'
                                    and date < ( select value::date from nielsen_meta where id = 1 )
                                order by date
                            ) q
                            group by song_id, weekly
                        ) q
                    ) q
                ) q
                group by song_id
            ) q
            left join nielsen_song.__song m on q.song_id = m.song_id
            where rolling_pct_chg > 0
                and num_positive_weeks > 4
            order by num_positive_weeks desc, rolling_pct_chg desc
        """
        df = self.db.execute(string)

        df.to_csv(self.reports_fullfiles['song_8_week_growth'], index=False)
    
    def report_genres(self):

        string = """
            select
                c.genre_id,
                c.rnk,
                c.num_positive_weeks,
                m.genre,
                c.should_notify::bool as is_new,
                st.tw_streams,
                st.volume,
                st.growth,
                st.artists_count,
                st.avg_tw_streams as avg_tw_streams_per_artist
            from nielsen_genres.ltg_chart c
            left join nielsen_genres.__stats st on c.genre_id = st.genre_id
            left join nielsen_genres.meta m on c.genre_id = m.id
            where is_top_100 is true
            order by c.rnk
        """
        df = self.db.execute(string)

        df.to_csv(self.reports_fullfiles['growing_genres'], index=False)

        string = """
            select
                a.artist_id,
                a.genre_id,
                g.genre,
                m.artist,
                m.tw_streams,
                m.pct_chg,
                a.rnk as genre_rnk,
                a.is_top_10,
                a.is_top_50,
                sp.spotify_copyrights,
                m.signed
            from nielsen_genres.artist_tw_chart a
            left join nielsen_artist.__artist m on a.artist_id = m.artist_id
            left join nielsen_artist.spotify sp on m.artist_id = sp.artist_id
            left join nielsen_genres.meta g on a.genre_id = g.id
            where should_notify::bool is true
                and m.tw_streams > 0
        """
        df = self.db.execute(string)

        df.to_csv(self.reports_fullfiles['new_artists_in_genres_tw_streams'], index=False)
    
    def report_nielsenWeeklyAudio(self):

        # Read in the data
        df = pd.read_csv(self.fullfiles['song'], encoding='UTF-16')

        # Rename the remaining columns for consistency and database usage
        renameable = {
            'TW Rank': 'tw_rank',
            'LW Rank': 'lw_rank',
            'Artist': 'artist',
            'Title': 'title',
            'Unified Song Id': 'unified_song_id',
            'Label Abbrev': 'label',
            'CoreGenre': 'core_genre',
            'Top ISRC': 'isrc',
            'Release_date': 'release_date',
            'TW On-Demand Audio Streams': 'tw_oda_streams',
            'LW On-Demand Audio Streams': 'lw_oda_streams',
            'L2W_On_Demand_Audio_Streams': 'l2w_oda_streams',
            'Weekly %change On-Demand Audio Streams': 'weekly_pct_chg_oda_streams',
            'YTD On-Demand Audio Streams': 'ytd_oda_streams',
            'RTD On-Demand Audio Streams': 'rtd_oda_streams',
            'RTD On-Demand Audio Streams - Premium': 'rtd_oda_streams_premium',
            'RTD On-Demand Audio Streams - Ad Supported': 'rtd_oda_streams_ad_supported',
            'WTD Building ODA (Friday-Thursday)': 'wtd_building_fri_thurs',
            '7-day Rolling ODA': 'tw_rolling_oda',
            'pre-7days rolling ODA': 'lw_rolling_oda',
            'TW Digital Track Sales': 'tw_digital_track_sales',
            'YTD Digital Track Sales': 'ytd_digital_track_sales',
            'ATD Digital Track Sales': 'atd_digital_track_sales',
            'TW On-Demand Video': 'tw_odv',
            'LW On-Demand Video': 'lw_odv',
            'YTD On-Demand Video': 'ytd_odv',
            'ATD On-Demand Video': 'atd_odv'
        }

        df = df.rename(columns=renameable)

        # Drop rows that have a null unified_song_id
        df = df[~df['unified_song_id'].isnull()].reset_index(drop=True)

        # Sometimes we have unified_song_id duplicates from nielsen, ignore these.
        df = df.drop_duplicates(subset='unified_song_id').reset_index(drop=True)

        # We're actually going to drop the release date because we're going to get it from the database later
        df = df.drop(columns=['release_date'])

        # Rename / clean daily streaming columns
        df = df[df.columns.drop(list(df.filter(regex=' - Ad Supported ODA')))]
        df = df[df.columns.drop(list(df.filter(regex=' - Premium ODA')))]
        df.columns = df.columns.str.replace(' - Total ODA', '')

        # Clean the types
        df = df.astype({
            'tw_rank': 'int',
            'artist': 'str',
            'title': 'str',
            'unified_song_id': 'int',
            'label': 'str',
            'core_genre': 'str',
            'isrc': 'str',
            'tw_oda_streams': 'int',
            'l2w_oda_streams': 'int',
            'rtd_oda_streams': 'int',
            'rtd_oda_streams_premium': 'int',
            'rtd_oda_streams_ad_supported': 'int',
            'tw_digital_track_sales': 'int',
            'atd_digital_track_sales': 'int',
            'tw_odv': 'int',
            'atd_odv': 'int'
        }).astype({ 'unified_song_id': 'str' })

        # Generate the chart week column names
        day_of_week = datetime.today().strftime('%A')
        delta = {
            'Sunday': 3,
            'Monday': 4,
            'Tuesday': 5,
            'Wednesday': 6,
            'Thursday': 7,
            'Friday': 8,
            'Saturday': 9
        }

        last_week_chart_week_end = datetime.today() - timedelta(delta[day_of_week])

        dateCols = []
        for i in range(7):
            dateCols.append((last_week_chart_week_end - timedelta(i)).strftime('%m/%d/%Y'))
            
        # Must be doing >= 50,000 streams
        df = df[df['tw_oda_streams'] >= 50000].reset_index(drop=True)

        # Attach report to the existing data in the database
        string = """
            create temp table tmp_unified_song_ids (
                unified_song_id text
            );
        """
        self.db.execute(string)
        self.db.big_insert(df[['unified_song_id']], 'tmp_unified_song_ids')

        string = """
            select
                m.song_id,
                m.artist_id,
                m.unified_song_id,
                m.release_date,
                m.signed,
                asp.genres,
                sp.copyrights,
                sp.spotify_track_id,
                sp.spotify_album_id,
                sp.instrumentalness,
                sp.energy,
                sp.speechiness,
                sp.acousticness,
                sp.tempo
            from nielsen_song.__song m
            join tmp_unified_song_ids u on m.unified_song_id = u.unified_song_id
            left join nielsen_song.spotify sp on m.song_id = sp.song_id
            left join nielsen_artist.spotify asp on m.artist_id = asp.artist_id
        """
        data = self.db.execute(string)

        string = 'drop table tmp_unified_song_ids'
        self.db.execute(string)
        df = pd.merge(df, data, on='unified_song_id', how='inner')

        # Drop anything signed
        df = df[df['signed'] != True].reset_index(drop=True)

        # Add a bunch of extra stats

        # Rank change
        df['rank_chg'] = df['lw_rank'] - df['tw_rank']
        df.loc[df['lw_rank'] == 0, 'rank_chg'] = 'New'

        # Percent change lw -> tw
        df['pct_chg'] = df['tw_oda_streams'].div(df['lw_oda_streams']) - 1

        # 3 Week Acceleration
        df['three_wk_acc'] = df['tw_oda_streams'].div(df['l2w_oda_streams']).pow(0.5) - 1

        # 7 Day Acceleration
        df['seven_day_acc'] = df[dateCols[0]].div(df[dateCols[6]]).pow(1 / 6) - 1

        # Filter
        mask = (
            (
                (df['pct_chg'] >= 0.25) & \
                (df['tw_oda_streams'] >= 200000) & \
                (df['lw_oda_streams'] >= 5000)
            ) | \
            (
                (df['pct_chg'] >= 0.35) & \
                (df['tw_oda_streams'] >= 100000) & \
                (df['lw_oda_streams'] >= 5000)
            ) | \
            (
                (df['three_wk_acc'] >= 0.15) & \
                (df['tw_oda_streams'] >= 100000) & \
                (df['l2w_oda_streams'] >= 1000) & \
                (df['pct_chg'] >= 0.15)
            ) | \
            (
                (df['seven_day_acc'] >= 0.1) & \
                (df['tw_oda_streams'] >= 100000) & \
                (df[dateCols[6]] > 1000)
            ) | \
            (
                (df['three_wk_acc'] >= 0.2) & \
                (df['tw_oda_streams'] >= 100000) & \
                (df['seven_day_acc'] > 0) & \
                (df['pct_chg'] >= 0.25)
            ) | \
            (
                (df['tw_oda_streams'] >= 50000) & \
                (df['tw_oda_streams'] <= 100000) & \
                (df['seven_day_acc'] >= 0.15) & \
                (df[dateCols[6]] > 1000)
            ) | \
            (
                (df['three_wk_acc'] >= 0.15) & \
                (df['tw_oda_streams'] >= 50000) & \
                (df['tw_oda_streams'] <= 100000) & \
                (df['seven_day_acc'] > 0) & \
                (df['pct_chg'] >= 0.4)
            ) | \
            (
                (df['lw_oda_streams'] < 5000) & \
                (df['tw_oda_streams'] >= 100000)
            )
        )

        df = df[mask].reset_index(drop=True)

        # Extract only the relevant columns
        cols = [
            'unified_song_id',
            'artist',
            'title',
            'label',
            'genres',
            'core_genre',
            'release_date',
            'copyrights',
            'instrumentalness',
            'three_wk_acc',
            'tw_oda_streams',
            'pct_chg',
            'lw_oda_streams',
            'l2w_oda_streams',
            'ytd_oda_streams',
            'seven_day_acc',
            *dateCols,
            'tw_odv',
            'lw_odv',
            'ytd_odv',
            'atd_odv'
        ]

        df = df[cols].reset_index(drop=True)

        df.to_csv(self.reports_fullfiles['nielsen_weekly_audio'], index=False)
    
    def report_artistSocialGrowth(self):

        string = """
            select *
            from social_charts.ig_follower_growth
            order by ig_growth_score desc
        """
        df = self.db.execute(string)
        df.to_csv(self.reports_fullfiles['ig_follower_growth'], index=False)

        string = """
            select *
            from social_charts.sp_follower_growth
            order by sp_growth_score desc
        """
        df = self.db.execute(string)
        df.to_csv(self.reports_fullfiles['sp_follower_growth'], index=False)

        string = """
            select *
            from social_charts.tt_follower_growth
            order by tt_growth_score desc
        """
        df = self.db.execute(string)
        df.to_csv(self.reports_fullfiles['tt_follower_growth'], index=False)
    
    def emailReports(self):

        def add_file(files, fullfile):
            if os.path.exists(fullfile):
                files.append({
                    'path': fullfile,
                    'filename': os.path.basename(fullfile)
                })

        files = []
        add_file(files, self.reports_fullfiles['genius_scrape'])
        add_file(files, self.reports_fullfiles['nielsen_daily_audio'])
        add_file(files, self.reports_fullfiles['shazam_by_market'])
        add_file(files, self.reports_fullfiles['shazam_by_market_streams'])
        add_file(files, self.reports_fullfiles['shazam_rank_by_country'])
        add_file(files, self.reports_fullfiles['spotify_artist_stat_growth'])
        add_file(files, self.reports_fullfiles['artist_8_week_growth'])
        add_file(files, self.reports_fullfiles['song_8_week_growth'])
        add_file(files, self.reports_fullfiles['growing_genres'])
        add_file(files, self.reports_fullfiles['nielsen_weekly_audio'])
        add_file(files, self.reports_fullfiles['new_artists_in_genres_tw_streams'])
        add_file(files, self.reports_fullfiles['ig_follower_growth'])
        add_file(files, self.reports_fullfiles['sp_follower_growth'])
        add_file(files, self.reports_fullfiles['tt_follower_growth'])

        recipients = [
            'alec.mather@rcarecords.com',
            'aaron.dombey@rcarecords.com',
            'karl.fricker@rcarecords.com'
        ]

        if self.settings['is_testing']:
            recipients = [ 'alec.mather@rcarecords.com' ]

        rapidApi = RapidApi()
        quote = rapidApi.getInspirationalQuote()

        self.email.send(recipients, 'Daily Reports', f'Quote of the day:\n{quote}', files)

    def build(self):

        self.add_function(self.downloadFiles, 'Download Files')
        self.add_function(self.validateSession, 'Validate Session')

        """
            Stage 1:
                - processArtists | Process artist file
                - processSongs | Process song file
                - updateRecentDate | Update the most recent date of data received.
        """
        self.add_function(self.processArtists, 'Process Artists')
        self.add_function(self.processSongs, 'Process Songs')
        self.add_function(self.updateRecentDate, 'Update Recent Date')

        """
            Stage 2:
                - refreshStats | Compute our statistics (tw_streams, lw_streams, pct_chg, tracks_count[projects only])
                    = Depends on processArtists, processSongs, updateRecentDate
                    = For artists / songs / projects
                    = Hold off on genres for now because we need the spotify information on new artists before we can fully connect genres.
                - cacheSpotifySongs | Cache spotify song information
                    = Depends on processSongs
                    = Caches spotify information about songs
        """
        self.add_function(self.refreshStats, 'Refresh Stats')
        self.add_function(self.cacheSpotifySongs, 'Cache Spotify Songs', error_on_failure=False)

        """
            Stage 3:
                - refreshArtistTracks | Refresh the map between artists and their songs
                    = Depends on refreshStats
                        : The rnk column comes from tw_streams in stats
                - cacheSpotifyAlbums | Cache spotify info about albums in our db
                    = Depends on cacheSpotifySongs
                        : Uses the associated albums to each spotify song to cache
                
        """
        self.add_function(self.refreshArtistTracks, 'Refresh Artist Tracks')
        self.add_function(self.cacheSpotifyAlbums, 'Cache Spotify Albums', error_on_failure=False)

        """
            Stage 4:
                - cacheSpotifyArtists | Cache spotify artist info
                    = Depends on refreshArtistTracks / cacheSpotifySongs
                        : Small optimization by connecting artists to songs and their spotify artist ids
        """
        self.add_function(self.cacheSpotifyArtists, 'Cache Spotify Artists', error_on_failure=False)


        """
            Stage 5:
                - cacheChartmetricIds | Cache map between artist ids and chartmetric social ids
                    = Depends on spotify artist id info
                - updateGenres | Cache genre information that connects artist streaming -> spotify genres
                    = Depends on spotify artist genres
                - filterSignedFromSpotifyCopyrights | Use spotify copyrights to filter signed artists
                    = Depends on cacheSpotifySongs, cacheSpotifyArtists
        """
        self.add_function(self.cacheChartmetricIds, 'Cache Chartmetric Ids', error_on_failure=False)
        self.add_function(self.updateGenres, 'Update Genres')
        self.add_function(self.filterSignedFromSpotifyCopyrights, 'Filter Signed from Spotify Copyrights')

        """
            Stage 6:
                - refreshReportsRecent | Refresh the most recent report for songs and artists
                    = Depends on filterSignedFromSpotifyCopyrights
                - recordGenreCharts | Record where everyone sits in their genres ranks
                    = Depends on updateGenres, refreshStats
        """
        self.add_function(self.refreshReportsRecent, 'Refresh Reports Recent')
        self.add_function(self.refreshCharts, 'Record Genre Charts')

        """
            Stage 7:
                - refreshDailyReport | Daily song report for aaron
                    = Depends on refreshStats, cacheSpotifySongs, refreshReportsRecent
        """
        self.add_function(self.refreshDailyReport, 'Refresh Daily Report')

        """
            Stage 8:
                - refreshSimpleViews | Refresh materialized aggregate views of artists/songs
                    = Depends on refreshReportsRecent, spotify artist/song data, refreshStats
                - refreshNotifications | Refresh notifications to send out
                    = Depends on pretty much everything
        """
        self.add_function(self.refreshSimpleViews, 'Refresh Simple Views')
        self.add_function(self.refreshNotifications, 'Refresh Notifications')

        # Commit
        self.add_function(self.db.commit, 'Commit')

        """
            Functions in this section are meant to be relatively separate from anything before hand. The reason is because
            these functions are sometimes quite costly in terms of "time". So we end the previous section by committing our
            database changes (which updates graphitti and has priority) and then we move into this section and finish the
            rest of the pipeline. Most of what's in this section has to do with reporting, which all gets emailed out at
            the end of the pipeline.

            Stage 9:
                - updateSpotifyCharts | Updates spotify charts
                    = Depends on refreshSimpleViews
                - archiveNielsenFiles | Upload nielsen zip file to s3 bucket (does not delete the local copy)
        """
        self.add_function(self.updateSpotifyCharts, 'Update Spotify Charts', error_on_failure=False)
        self.add_function(self.archiveNielsenFiles, 'Archive Nielsen Files', error_on_failure=False)
        self.add_function(self.updateArtistSocialCharts, 'Update Artist Social Charts', error_on_failure=False)

        """
            Reporting

            This section now just has to do with creating and sending out reports according to our schedule.
        """

        self.add_function(self.report_genius, 'Genius Scrape', error_on_failure=False)
        self.add_function(self.report_spotifyArtistStatGrowth, 'Spotify Artist Stat Growth', error_on_failure=False)
        self.add_function(self.report_dailySongs, 'Daily Songs', error_on_failure=False)
        self.add_function(self.report_shazam, 'Shazam', error_on_failure=False)
        self.add_function(self.report_artist8WeekGrowth, 'Artist 8 Week Growth', error_on_failure=False)
        self.add_function(self.report_song8WeekGrowth, 'Song 8 Week Growth', error_on_failure=False)
        self.add_function(self.report_genres, 'Growing Genres Report', error_on_failure=False)
        self.add_function(self.report_nielsenWeeklyAudio, 'Nielsen Weekly Audio', error_on_failure=False)
        self.add_function(self.report_artistSocialGrowth, 'Report Artist Social Growth', error_on_failure=False)
        self.add_function(self.emailReports, 'Email Reports', error_on_failure=False)

    def test_build(self):

        # Always set the settings to test regardless of command line arguments
        self.settings['is_testing'] = True

        self.add_function(self.downloadFiles, 'Download Files')
        self.add_function(self.validateSession, 'Validate Session')
        # self.add_function(self.processArtists, 'Process Artists')
        # self.add_function(self.processSongs, 'Process Songs')
        # self.add_function(self.testDbInsert, 'Test Db Insert')

        # self.add_function(self.report_genius, 'Genius Scrape', error_on_failure=False)
        # self.add_function(self.report_spotifyArtistStatGrowth, 'Spotify Artist Stat Growth', error_on_failure=False)
        # self.add_function(self.report_dailySongs, 'Daily Songs', error_on_failure=False)
        # self.add_function(self.report_shazam, 'Shazam', error_on_failure=False)
        # self.add_function(self.report_artist8WeekGrowth, 'Artist 8 Week Growth', error_on_failure=False)
        # self.add_function(self.report_song8WeekGrowth, 'Song 8 Week Growth', error_on_failure=False)
        # self.add_function(self.report_nielsenWeeklyAudio, 'Nielsen Weekly Audio', error_on_failure=False)
        # self.add_function(self.report_genres, 'Genres Reports', error_on_failure=False)
        self.add_function(self.updateArtistSocialCharts, 'Update Artist Social Charts')
        self.add_function(self.report_artistSocialGrowth, 'Report Artist Social Growth')

        self.add_function(self.emailReports, 'Email Reports', error_on_failure=False)