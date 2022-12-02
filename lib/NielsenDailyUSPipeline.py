from .env import LOCAL_ARCHIVE_FOLDER, LOCAL_DOWNLOAD_FOLDER, REPORTS_FOLDER
from .PipelineBase import PipelineBase
from .functions import chunker
from .Sftp import Sftp
from .Spotify import Spotify
from zipfile import ZipFile
from .Fuzz import Fuzz
from .Db import Db
from requests.exceptions import ReadTimeout
from datetime import datetime, timedelta
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

# Convert a string to a date object
def str2Date(s):
    try:
        return datetime.strptime(s, '%m/%d/%Y')
    except ValueError:
        try:
            return datetime.strptime(s, '%Y-%m-%d')
        except ValueError:
            return False

def getDateCols(cols):
    
    idx = []
    for col in cols:
        d = str2Date(col)
        if d:
            idx.append(col)
    return idx

# Convert columns into correct date indicies
def getDateIndicies(cols):
    idx = []
    for col in cols:
        d = str2Date(col)
        if d:
            idx.append(col)
    return idx

# Convert to property datetime format for merging later
def tDate(d):
    m, d, y = d.split('/')
    return f'{y}-{m}-{d}'

def transformSpotifyArtistObject(artist):

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

    def findSignedByCopyrights(self, df: pd.DataFrame):

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
     
    def basicSignedCheck(self, df: pd.DataFrame):

        """
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

    def cleanArtists(self, df: pd.DataFrame):
        
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
        streams['streams']  = streams['streams'].fillna.astype('int')
        
        return meta, streams

    # Apply another layer of detecting 'signed' with a running list of signed artists
    def filterBySignedArtistsList(self, df: pd.DataFrame):
        
        # Read in the signed artists that are tracked
        artists_df = self.db.execute('select * from misc.signed_artists')

        if artists_df is None:
            raise Exception('Missing signed artists template')

        artists = artists_df['artist'].values
        
        # Check if they're in our signed list
        df.loc[df['artist'].isin(artists), 'signed'] = True
        
        return df

    def prepareArtistData(self, df: pd.DataFrame):
        
        # Clean & standardize data
        meta, streams = self.cleanArtists(df)
        
        # Add the 'signed' status to the artist
        meta = self.filterBySignedArtistsList(meta)
        
        return meta, streams

    def artistsDbUpdates(self, meta: pd.DataFrame, streams: pd.DataFrame):

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

    def filterSignedSongs(self, df: pd.DataFrame):
        
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

    def cleanSongs(self, df: pd.DataFrame):
        
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

    def appendToSignedArtistList(self, df: pd.DataFrame):
        
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

    def prepareSongData(self, df: pd.DataFrame):
        
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
        streams['streams']  = streams['streams'].fillna.astype('int')
        streams['premium']  = streams['premium'].fillna.astype('int')
        streams['ad_supported']  = streams['ad_supported'].fillna.astype('int')
        
        return meta, streams

    def songsDbUpdates(self, meta: pd.DataFrame, streams: pd.DataFrame):

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
            insert into nielsen_song.meta (artist, title, unified_song_id, label, core_genre, release_date, isrc, is_global)
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

    def getSpotifySongs(self, df: pd.DataFrame):

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

    def bulkGetSpotifyArtistInfo(self, df: pd.DataFrame, spotify: Spotify) -> pd.DataFrame:

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

    def getSpotifyArtistInfo(self, df: pd.DataFrame, spotify: Spotify) -> pd.DataFrame:

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

    def getSpotifyPopularTrackId(self, df: pd.DataFrame, spotify: Spotify) -> pd.DataFrame:

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
                pd.Series((None, None))

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

    def getSpotifyAlbumInfo(self, df: pd.DataFrame, spotify: Spotify) -> pd.DataFrame:

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

        # Create a connection to the reporting db
        reporting_db = Db('reporting_db')
        reporting_db.connect()

        # First get spotify artist ids from our artist cache
        string = """
            select s.artist_id, s.spotify_artist_id, m.unified_artist_id
            from nielsen_artist.spotify s
            left join nielsen_artist.meta m on m.id = s.artist_id
            where s.spotify_artist_id is not null
                and artist_id not in (select artist_id from nielsen_artist.cm_map)
            limit 100
        """
        df = self.db.execute(string)
        if df is None:
            return

        # Get the charmetric mapping
        string = """
            with temp as (
                select
                    cm_artist as target_id,
                    spotify_artist_id as spotify_id
                from chartmetric_raw.spotify_artist
                where spotify_artist_id in %(spotify_ids)s
            ), t as (
                select temp.*, cm.account_id, cm.type
                from temp
                left join chartmetric_raw.cm_url cm
                    on cm.target_id = temp.target_id
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
                t.spotify_id as spotify_artist_id,
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
            left join chartmetric_raw.spotify_artist sa on t.spotify_id = sa.spotify_artist_id
            group by
                t.target_id, t.spotify_id,
                sa.id,
                instagram_id, youtube_id, tiktok_id,
                shazam_id, twitter_id, genius_id, gtrends_id, soundcloud_id, twitch_id
        """
        params = { 'spotify_ids': tuple(df['spotify_artist_id'].values) }
        data = reporting_db.execute(string, params)
        if data is None:
            return

        # Merge onto our database map
        df = pd.merge(df, data, how='left')

        # Insert into our db
        self.db.big_insert(df, 'nielsen_artist.cm_map')

        # Disconnect from the reporting db
        reporting_db.disconnect()

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

    def recordGenreCharts(self):

        """
            Record where everyone sits in their genres.
        """

        string = """
            -- LTG
            insert into nielsen_genres.chart_ltg_streams_history (artist_id, genre_id, num_positive_weeks, rnk)
            select
                artist_id,
                genre_id,
                num_positive_weeks,
                row_number() over (partition by genre_id order by num_positive_weeks desc, tw_streams desc) as rnk
            from (
                select
                    genre_id,
                    artist_id,
                    tw_streams,
                    sum(is_greater) as num_positive_weeks
                from (
                    select *,
                        case
                            when streams > streams_prev then 1
                            else 0
                        end as is_greater
                    from (
                        select *, lag(streams, 1) over (partition by artist_id order by weekly) as streams_prev
                        from (
                            select
                                a.genre_id,
                                a.artist_id,
                                st.tw_streams,
                                date_trunc('week', s.date) as weekly,
                                sum(s.streams) as streams
                            from nielsen_genres.artists a
                            left join nielsen_artist.streams s on a.artist_id = s.artist_id
                            join nielsen_artist.stats st on a.artist_id = st.artist_id
                            where st.tw_streams > 30000
                            group by a.genre_id, a.artist_id, st.tw_streams, weekly
                        ) q
                    ) q
                ) q
                group by genre_id, artist_id, tw_streams
            ) q
            order by num_positive_weeks desc;
            -- End LTG

            -- LTG New
            with recent_dates as (
                select distinct date
                from nielsen_genres.chart_ltg_streams_history
                order by date desc
                limit 2
            ), recent_data as (
                select *
                from nielsen_genres.chart_ltg_streams_history
                where date = (select date from recent_dates order by date desc limit 1)
            ), offset_data as (
                select *
                from nielsen_genres.chart_ltg_streams_history
                where date = (select date from recent_dates order by date desc limit 1 offset 1)
            )

            insert into nielsen_genres.chart_ltg_streams_new(genre_id, artist_id, rnk)
            select rd.genre_id, rd.artist_id, rd.rnk
            from recent_data rd
            full outer join offset_data od
                on rd.artist_id = od.artist_id
                and rd.genre_id = od.genre_id
            where od.artist_id is null
                and rd.rnk < 50;
            -- End LTG New
            
            -- TW
            insert into nielsen_genres.chart_tw_streams_history (artist_id, genre_id, rnk)
            select
                a.artist_id,
                a.genre_id,
                row_number() over (partition by genre order by st.tw_streams desc nulls last) as rnk
            from nielsen_genres.artists a
            left join nielsen_artist.meta m on m.id = a.artist_id
            join nielsen_artist.stats st on a.artist_id = st.artist_id;
            -- End TW

            -- TW New
            with recent_dates as (
                select distinct date
                from nielsen_genres.chart_tw_streams_history
                order by date desc
                limit 2
            ), recent_data as (
                select *
                from nielsen_genres.chart_tw_streams_history
                where date = (select date from recent_dates order by date desc limit 1)
            ), offset_data as (
                select *
                from nielsen_genres.chart_tw_streams_history
                where date = (select date from recent_dates order by date desc limit 1 offset 1)
            )

            insert into nielsen_genres.chart_tw_streams_new(genre_id, artist_id, rnk)
            select rd.genre_id, rd.artist_id, rd.rnk
            from recent_data rd
            full outer join offset_data od
                on rd.artist_id = od.artist_id
                and rd.genre_id = od.genre_id
            where od.artist_id is null
                and rd.rnk < 50;
            -- End TW New
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
        self.aws.upload_s3(self.fullfiles['zip'], s3_fullfile)

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
        self.add_function(self.cacheSpotifySongs, 'Cache Spotify Songs')

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
        self.add_function(self.cacheSpotifyAlbums, 'Cache Spotify Albums')

        """
            Stage 4:
                - cacheSpotifyArtists | Cache spotify artist info
                    = Depends on refreshArtistTracks / cacheSpotifySongs
                        : Small optimization by connecting artists to songs and their spotify artist ids
        """
        self.add_function(self.cacheSpotifyArtists, 'Cache Spotify Artists')


        """
            Stage 5:
                - cacheChartmetricIds | Cache map between artist ids and chartmetric social ids
                    = Depends on spotify artist id info
                - updateGenres | Cache genre information that connects artist streaming -> spotify genres
                    = Depends on spotify artist genres
                - filterSignedFromSpotifyCopyrights | Use spotify copyrights to filter signed artists
                    = Depends on cacheSpotifySongs, cacheSpotifyArtists
        """
        self.add_function(self.cacheChartmetricIds, 'Cahce Chartmetric Ids')
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
        self.add_function(self.recordGenreCharts, 'Record Genre Charts')

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
        """
        self.add_function(self.refreshSimpleViews, 'Refresh Simple Views')

        """
            Stage 9:
                - updateSpotifyCharts | Updates spotify charts
                    = Depends on refreshSimpleViews
        """
        self.add_function(self.updateSpotifyCharts, 'Update Spotify Charts')

        self.add_function(self.archiveNielsenFiles, 'Archive Nielsen Files')

    def test_build(self):

        # Always set the settings to test regardless of command line arguments
        self.settings['is_testing'] = True

        self.add_function(self.downloadFiles, 'Download Files')
        self.add_function(self.validateSession, 'Validate Session')
        self.add_function(self.processArtists, 'Process Artists')
        self.add_function(self.processSongs, 'Process Songs')
        self.add_function(self.testDbInsert, 'Test Db Insert')
        self.add_function(self.deleteFiles, 'Delete Files')