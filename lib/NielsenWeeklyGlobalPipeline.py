from .PipelineBase import PipelineBase
from .Sftp import Sftp
from .Sftp import Sftp
from datetime import datetime, timedelta
import pandas as pd
import os


GLOBAL_COUNTRY_NAMES = {
    'eu_weekly': [ 'Austria', 'Belgium', 'Croatia', 'Czech_Republic', 'Denmark', 'Finland', 'France', 'Germany', 'Iceland', 'Ireland', 'Italy', 'Luxembourg', 'Netherlands', 'Norway', 'Poland', 'Portugal', 'Spain', 'Sweden', 'Switzerland', 'United_Kingdom' ],
    'ne_asia_weekly': [ 'Japan', 'Korea' ],
    'se_asia_weekly': [ 'Australia', 'Hong_Kong', 'Indonesia', 'Malaysia', 'New_Zealand', 'Philippines', 'Singapore', 'Taiwan', 'Thailand', 'Vietnam' ],
    'lat_am_weekly': [ 'Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Mexico', 'Peru' ],
    'emerging_weekly': [ 'Greece', 'Hungary', 'India', 'Romania', 'Slovakia', 'South_Africa', 'Turkey' ]
}

GLOBAL_SERVER_NAMES = [
    'eu_weekly',
    'ne_asia_weekly',
    'se_asia_weekly',
    'lat_am_weekly',
    'emerging_weekly'
]

ARTISTS_STR_INDICATOR = '_Top20kArtist_'
SONGS_STR_INDICATOR = '_Top50kSong_'


class NielsenWeeklyGlobalPipeline(PipelineBase):

    def __init__(self, db_name):
        PipelineBase.__init__(self, db_name)
        self.sftp_conn_name = None
        self.sftp_conn = None
    
    def getNewWeeklyFiles(self):

        def parse_year_week(year_week_string):

            year = int(year_week_string[:4])
            week = int(year_week_string[4:])
            
            # Calculate the date of the first day of the year
            first_day = datetime(year, 1, 1)

            # Check if the first day of the year is a Monday; if not, find the next Monday
            if first_day.weekday() > 0:
                first_day = first_day + timedelta(days=(7 - first_day.weekday()))

            # Calculate the date of the first day of the given week number
            date_object = first_day + timedelta(weeks=(week - 1))

            return date_object

        def parse_filename(filename, indicator, server_name):

            filename_base = filename.replace('.tsv', '')
            country, date = filename_base.split(indicator)
            date = parse_year_week(date)

            return {
                'filename': filename,
                'name': country.lower(),
                'display_name': country.replace('_', ' '),
                'date': date,
                'server_name': server_name
            }

        artist_filenames = []
        song_filenames = []

        for server_name in GLOBAL_SERVER_NAMES:

            sftp = Sftp(server_name)
            filenames = sftp.list()
            artist_filenames = [ *artist_filenames, *[ parse_filename(i, ARTISTS_STR_INDICATOR, server_name) for i in filenames if ARTISTS_STR_INDICATOR in i and '__NO_DATA' not in i and '.tsv' in i ] ]
            song_filenames = [ *song_filenames, *[ parse_filename(i, SONGS_STR_INDICATOR, server_name) for i in filenames if SONGS_STR_INDICATOR in i and '__NO_DATA' not in i and '.tsv' in i ] ]

        artist_filenames = pd.DataFrame(artist_filenames)
        song_filenames = pd.DataFrame(song_filenames)

        artist_filenames = artist_filenames.sort_values(by=['name', 'date'], ascending=[True, False]).drop_duplicates(subset=['name'], keep='first').reset_index(drop=True)
        song_filenames = song_filenames.sort_values(by=['name', 'date'], ascending=[True, False]).drop_duplicates(subset=['name'], keep='first').reset_index(drop=True)

        # Compare the most recent files available to the most recent date stored in the database
        string = """
            create temp table artist_files (
                name text,
                date date
            );
        """
        self.db.execute(string)
        self.db.big_insert(artist_filenames[['name', 'date']], 'artist_files')

        string = """
            create temp table song_files (
                name text,
                date date
            );
        """
        self.db.execute(string)
        self.db.big_insert(song_filenames[['name', 'date']], 'song_files')

        string = """
            select
                m.id,
                af.name
            from artist_files af
            join nielsen_global.meta m on af.name = m.name and (af.date > m.artists_last_processed or m.artists_last_processed is null)
        """
        new_artist_files = self.db.execute(string)

        string = """
            select
                m.id,
                af.name
            from song_files af
            join nielsen_global.meta m on af.name = m.name and (af.date > m.songs_last_processed or m.songs_last_processed is null)
        """
        new_song_files = self.db.execute(string)

        string = """
            drop table artist_files;
            drop table song_files;
        """

        artists = pd.merge(artist_filenames, new_artist_files)
        songs = pd.merge(song_filenames, new_song_files)

        artists['type'] = 'artists'
        songs['type'] = 'songs'

        return pd.concat([ artists, songs ])

    def cleanArtists(self, df):

        # Rename columns
        # You do actually need to do this first because the 'Digital Song Sales' regex later, just trust me
        atd_name = df.filter(regex='Streaming On-Demand Total - ATD').columns.tolist()[0]
        rename_columns = {
            'Country': 'country',
            'UnifiedArtistID': 'unified_artist_id',
            'Digital Song Sales - TP': 'digital_song_sales_tw',
            'Streaming On-Demand Total - TP': 'tw_streams',
            'Streaming On-Demand Total - % Change LP': 'pct_chg',
            'Streaming On-Demand Total - LP': 'lw_streams',
            'Streaming On-Demand Total - YTD': 'ytd_streams',
            atd_name: 'rtd_streams'
        }
        df.rename(columns=rename_columns, inplace=True)

        # Drop unnecessary columns
        drop_columns = [
            'Country Code',
            'WeekID',
            'WeekEndingDate',
            'Artist',
            'Rank',
            'Digital Song Sales - % Change LP',
            'Digital Song Sales - LP',
            'Digital Song Sales - YTD',
            *df.filter(regex='Digital Song Sales').columns.tolist()
        ]
        df.drop(columns=drop_columns, inplace=True)

        # Drop duplicates just in case
        df = df.drop_duplicates(subset='unified_artist_id').reset_index(drop=True)

        # Transform the country name to what we have in our db so we can match it later
        df['country'] = df['country'].str.lower()

        # Remove the percent character from the 'pct_chg' column
        df['pct_chg'] = df['pct_chg'].str.replace('%', '')

        # Add the rank ourselves (we do this because their ranking system has duplicate ranks for ties)
        df['rnk'] = df.index + 1

        # Clean types
        df = df.astype({
            'country': 'str',
            'rnk': 'int',
            'unified_artist_id': 'str',
            'tw_streams': 'int',
            'pct_chg': 'float',
            'lw_streams': 'int',
            'ytd_streams': 'int',
            'rtd_streams': 'int',
            'digital_song_sales_tw': 'int'
        })

        return df

    def cleanSongs(self, df):

        # Rename columns
        # You do actually need to do this first because the 'Digital Song Sales' regex later, just trust me
        atd_name = df.filter(regex='Streaming On-Demand Total - ATD').columns.tolist()[0]
        rename_columns = {
            'Country': 'country',
            'UnifiedArtistID': 'unified_artist_id',
            'UnifiedSongID': 'unified_song_id',
            'Digital Song Sales - TP': 'digital_song_sales_tw',
            'Streaming On-Demand Total - TP': 'tw_streams',
            'Streaming On-Demand Total - % Change LP': 'pct_chg',
            'Streaming On-Demand Total - LP': 'lw_streams',
            'Streaming On-Demand Total - YTD': 'ytd_streams',
            atd_name: 'rtd_streams'
        }
        df.rename(columns=rename_columns, inplace=True)

        # Drop unnecessary columns
        drop_columns = [
            'Country Code',
            'WeekID',
            'WeekEndingDate',
            'Artist',
            'Title',
            'Top ISRC',
            'Rank',
            'Digital Song Sales - % Change LP',
            'Digital Song Sales - LP',
            'Digital Song Sales - YTD',
            *df.filter(regex='Digital Song Sales').columns.tolist()
        ]
        df.drop(columns=drop_columns, inplace=True)

        # Drop duplicates just in case
        df = df.drop_duplicates(subset=['unified_artist_id', 'unified_song_id']).reset_index(drop=True)

        # Transform the country name to what we have in our db so we can match it later
        df['country'] = df['country'].str.lower()

        # Remove the percent character from the 'pct_chg' column
        df['pct_chg'] = df['pct_chg'].str.replace('%', '')

        # Add the rank ourselves (we do this because their ranking system has duplicate ranks for ties)
        df['rnk'] = df.index + 1

        # Clean types
        df = df.astype({
            'country': 'str',
            'rnk': 'int',
            'unified_artist_id': 'str',
            'unified_song_id': 'str',
            'tw_streams': 'int',
            'pct_chg': 'float',
            'lw_streams': 'int',
            'ytd_streams': 'int',
            'rtd_streams': 'int',
            'digital_song_sales_tw': 'int'
        })

        return df

    def artistUpdates(self, df):

        # Now we need to get the global_id and the artist_id
        string = """
            create temp table tmp_artists (
                unified_artist_id text,
                country text
            );
        """
        self.db.execute(string)
        self.db.big_insert(df[['country', 'unified_artist_id']], 'tmp_artists')

        string = """
            select
                gm.id as global_id,
                m.id as artist_id,
                ta.country,
                ta.unified_artist_id
            from tmp_artists ta
            join nielsen_artist.meta m on ta.unified_artist_id = m.unified_artist_id
            join nielsen_global.meta gm on ta.country = gm.name
        """
        meta = self.db.execute(string)

        string = 'drop table tmp_artists'
        self.db.execute(string)

        # Add the new ids onto the dataframe
        df = pd.merge(df, meta, on=['country', 'unified_artist_id'])

        # Extract only the columns we need
        df = df[['global_id', 'artist_id', 'rnk', 'tw_streams', 'lw_streams', 'pct_chg', 'ytd_streams', 'rtd_streams', 'digital_song_sales_tw']]

        # Upsert new data
        string = """
            create temp table tmp_artists (
                global_id bigint,
                artist_id bigint,
                rnk int,
                tw_streams int,
                lw_streams int,
                pct_chg float,
                ytd_streams bigint,
                rtd_streams bigint,
                digital_song_sales_tw int
            );
        """
        self.db.execute(string)
        self.db.big_insert(df, 'tmp_artists')

        string = """
            insert into nielsen_artist.global_stats
            select
                global_id,
                artist_id,
                rnk,
                tw_streams,
                lw_streams,
                pct_chg,
                ytd_streams,
                rtd_streams,
                digital_song_sales_tw
            from tmp_artists
            on conflict (global_id, artist_id) do update
            set
                rnk = excluded.rnk,
                tw_streams = excluded.tw_streams,
                lw_streams = excluded.lw_streams,
                pct_chg = excluded.pct_chg,
                ytd_streams = excluded.ytd_streams,
                rtd_streams = excluded.rtd_streams,
                digital_song_sales_tw = excluded.digital_song_sales_tw
        """
        self.db.execute(string)

        string = 'drop table tmp_artists'
        self.db.execute(string)
        
    def songUpdates(self, df):

        # Now we need to get the global_id and the artist_id
        string = """
            create temp table tmp_songs (
                unified_artist_id text,
                unified_song_id text,
                country text
            );
        """
        self.db.execute(string)
        self.db.big_insert(df[['country', 'unified_artist_id', 'unified_song_id']], 'tmp_songs')

        string = """
            select
                gm.id as global_id,
                am.id as artist_id,
                sm.id as song_id,
                ts.country,
                ts.unified_artist_id,
                ts.unified_song_id
            from tmp_songs ts
            join nielsen_artist.meta am on ts.unified_artist_id = am.unified_artist_id
            join nielsen_song.meta sm on ts.unified_song_id = sm.unified_song_id
            join nielsen_global.meta gm on ts.country = gm.name
        """
        meta = self.db.execute(string)

        string = 'drop table tmp_songs'
        self.db.execute(string)

        # Add the new ids onto the dataframe
        df = pd.merge(df, meta, on=['country', 'unified_artist_id', 'unified_song_id'])

        # Extract only the columns we need
        df = df[['global_id', 'artist_id', 'song_id', 'rnk', 'tw_streams', 'lw_streams', 'pct_chg', 'ytd_streams', 'rtd_streams', 'digital_song_sales_tw']]

        # Upsert new data
        string = """
            create temp table tmp_songs (
                global_id bigint,
                artist_id bigint,
                song_id bigint,
                rnk int,
                tw_streams int,
                lw_streams int,
                pct_chg float,
                ytd_streams bigint,
                rtd_streams bigint,
                digital_song_sales_tw int
            );
        """
        self.db.execute(string)
        self.db.big_insert(df, 'tmp_songs')

        string = """
            insert into nielsen_song.global_stats
            select
                global_id,
                artist_id,
                song_id,
                rnk,
                tw_streams,
                lw_streams,
                pct_chg,
                ytd_streams,
                rtd_streams,
                digital_song_sales_tw
            from tmp_songs
            on conflict (global_id, artist_id, song_id) do update
            set
                rnk = excluded.rnk,
                tw_streams = excluded.tw_streams,
                lw_streams = excluded.lw_streams,
                pct_chg = excluded.pct_chg,
                ytd_streams = excluded.ytd_streams,
                rtd_streams = excluded.rtd_streams,
                digital_song_sales_tw = excluded.digital_song_sales_tw
        """
        self.db.execute(string)

        string = 'drop table tmp_songs'
        self.db.execute(string)

    def updateArtistsLastProcessed(self, file):

        string = """
            update nielsen_global.meta
            set artists_last_processed = %(date)s
            where name = %(name)s
        """
        params = {
            'date': file['date'].strftime('%Y-%m-%d'),
            'name': file['name']
        }
        self.db.execute(string, params)

    def updateSongsLastProcessed(self, file):

        string = """
            update nielsen_global.meta
            set songs_last_processed = %(date)s
            where name = %(name)s
        """
        params = {
            'date': file['date'].strftime('%Y-%m-%d'),
            'name': file['name']
        }
        self.db.execute(string, params)
    
    def processArtistFile(self, file):
        
        df = self.initFileProcess(file)

        df = self.cleanArtists(df)
        self.artistUpdates(df)

        self.updateArtistsLastProcessed(file)
        self.finishFileProcess(file)

    def processSongFile(self, file):
        
        df = self.initFileProcess(file)

        df = self.cleanSongs(df)
        self.songUpdates(df)

        self.updateSongsLastProcessed
        self.finishFileProcess(file)

    def initFileProcess(self, file):

        # Make sure that we are set to the right connection
        if self.sftp_conn is None or self.sftp_conn_name is None or file['server_name'] != self.sftp_conn_name:
            sftp = Sftp(file['server_name'])
            self.sftp_conn = sftp.connect()
            self.sftp_conn_name = file['server_name']

        # Download files from the server if they don't exist already
        fullfile = file['filename']
        if os.path.exists(fullfile) is False:
            self.sftp_conn.get(fullfile, fullfile)

        # Read, clean and update
        df = pd.read_csv(fullfile, delimiter='\t', encoding='UTF-16')

        return df
    
    def finishFileProcess(self, file):

        fullfile = file['filename']
        if os.path.exists(fullfile):
            os.remove(fullfile)

        self.commit()

    def addProcessFunc(self, file):
        
        processFunc = None
        if file['type'] == 'artists':
            processFunc = lambda: self.processArtistFile(file)
        elif file['type'] == 'songs':
            processFunc = lambda: self.processSongFile(file)

        if processFunc is None:
            raise Exception('Not a valid process func')

        self.add_function(processFunc, file['filename'])
    
    def build(self):
        
        files = self.getNewWeeklyFiles()
        print(f'Processing {len(files)} new files')

        files = files.sort_values(by='server_name').reset_index(drop=True).to_dict('records')

        for file in files.to_dict('records'):
            self.addProcessFunc(file)

    def test_build(self):
        pass