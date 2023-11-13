from .PipelineBase import PipelineBase
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
            'Artist': 'artist',
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
            'Rank',
            'Digital Song Sales - % Change LP',
            'Digital Song Sales - LP',
            'Digital Song Sales - YTD',
            *df.filter(regex='Digital Song Sales').columns.tolist()
        ]
        df.drop(columns=drop_columns, inplace=True)

        # If nielsen wrongly puts null for the artist name, we'll just default to unknown because that column is required
        df['artist'] = df['artist'].fillna('Unknown')

        # Drop duplicates just in case
        df = df.drop_duplicates(subset='unified_artist_id').reset_index(drop=True)

        # Transform the country name to what we have in our db so we can match it later
        df['country'] = df['country'].str.lower()
        df['country'] = df['country'].str.replace(' ', '_') # also for some reason "hong kong" needs underscores...?

        # Remove the percent character from the 'pct_chg' column
        df['pct_chg'] = df['pct_chg'].str.replace('%', '')

        # Add the rank ourselves (we do this because their ranking system has duplicate ranks for ties)
        df['rnk'] = df.index + 1

        # Clean types
        df = df.astype({
            'artist': 'str',
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
            'UnifiedSongID': 'unified_song_id',
            'Artist': 'artist',
            'Title': 'title',
            'Top ISRC': 'isrc',
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
            'UnifiedArtistID',
            'Country Code',
            'WeekID',
            'WeekEndingDate',
            'Rank',
            'Digital Song Sales - % Change LP',
            'Digital Song Sales - LP',
            'Digital Song Sales - YTD',
            *df.filter(regex='Digital Song Sales').columns.tolist()
        ]
        df.drop(columns=drop_columns, inplace=True)

        # If nielsen wrongly puts null for the artist name or title, we'll just default to unknown because those columns are required
        df['artist'] = df['artist'].fillna('Unknown')
        df['title'] = df['title'].fillna('Unknown')

        # Drop duplicates just in case
        df = df.drop_duplicates(subset=['unified_song_id'], keep='first').reset_index(drop=True)

        # Transform the country name to what we have in our db so we can match it later
        df['country'] = df['country'].str.lower()
        df['country'] = df['country'].str.replace(' ', '_')

        # Remove the percent character from the 'pct_chg' column
        df['pct_chg'] = df['pct_chg'].str.replace('%', '')

        # Add the rank ourselves (we do this because their ranking system has duplicate ranks for ties)
        df['rnk'] = df.index + 1

        # Clean types
        df = df.astype({
            'country': 'str',
            'rnk': 'int',
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

        string = """
            create temp table tmp_artists (
                country text,
                unified_artist_id text,
                artist text,
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
            insert into nielsen_artist.meta (artist, unified_artist_id, is_global)
            select artist, unified_artist_id, true from tmp_artists
            on conflict (unified_artist_id) do nothing;

            create temp table tmp_global_data as (
                select
                    gm.id as global_id,
                    m.id as artist_id,
                    ta.rnk,
                    ta.tw_streams,
                    ta.lw_streams,
                    ta.pct_chg,
                    ta.ytd_streams,
                    ta.rtd_streams,
                    ta.digital_song_sales_tw
                from tmp_artists ta
                left join nielsen_artist.meta m on ta.unified_artist_id = m.unified_artist_id
                left join nielsen_global.meta gm on ta.country = gm.name
            );

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
            from tmp_global_data
            on conflict (global_id, artist_id) do update
            set
                rnk = excluded.rnk,
                tw_streams = excluded.tw_streams,
                lw_streams = excluded.lw_streams,
                pct_chg = excluded.pct_chg,
                ytd_streams = excluded.ytd_streams,
                rtd_streams = excluded.rtd_streams,
                digital_song_sales_tw = excluded.digital_song_sales_tw;

            drop table tmp_global_data;
            drop table tmp_artists;
        """
        self.db.execute(string)
        
    def songUpdates(self, df):

        string = """
            create temp table tmp_songs (
                country text,
                unified_song_id text,
                artist text,
                title text,
                isrc text,
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
            insert into nielsen_song.meta (artist, title, unified_song_id, isrc, is_global)
            select artist, title, unified_song_id, isrc, true from tmp_songs
            on conflict (unified_song_id) do nothing;

            create temp table tmp_global_data as (
                select
                    gm.id as global_id,
                    m.id as song_id,
                    ta.rnk,
                    ta.tw_streams,
                    ta.lw_streams,
                    ta.pct_chg,
                    ta.ytd_streams,
                    ta.rtd_streams,
                    ta.digital_song_sales_tw
                from tmp_songs ta
                left join nielsen_song.meta m on ta.unified_song_id = m.unified_song_id
                left join nielsen_global.meta gm on ta.country = gm.name
            );

            insert into nielsen_song.global_stats
            select
                global_id,
                song_id,
                rnk,
                tw_streams,
                lw_streams,
                pct_chg,
                ytd_streams,
                rtd_streams,
                digital_song_sales_tw
            from tmp_global_data
            on conflict (global_id, song_id) do update
            set
                rnk = excluded.rnk,
                tw_streams = excluded.tw_streams,
                lw_streams = excluded.lw_streams,
                pct_chg = excluded.pct_chg,
                ytd_streams = excluded.ytd_streams,
                rtd_streams = excluded.rtd_streams,
                digital_song_sales_tw = excluded.digital_song_sales_tw;

            drop table tmp_global_data;
            drop table tmp_songs;
        """
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

        self.updateSongsLastProcessed(file)
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
    
    def updateGlobalAndExUs(self):

        string = """
            create temp table ex_us as (
                select
                    gm.id as global_id,
                    q.*,
                    row_number() over (order by q.tw_streams desc) as rnk,
                    case
                        when q.lw_streams = 0 then 0
                        else round(round(q.tw_streams::numeric - q.lw_streams::numeric) / q.lw_streams::numeric * 100, 2)
                    end as pct_chg
                from (
                    select
                        artist_id,
                        sum(tw_streams) as tw_streams,
                        sum(lw_streams) as lw_streams,
                        sum(ytd_streams) as ytd_streams,
                        sum(rtd_streams) as rtd_streams,
                        sum(digital_song_sales_tw) as digital_song_sales_tw
                    from nielsen_artist.global_stats
                    where global_id not in (67, 68)
                    group by artist_id
                ) q
                cross join ( select * from nielsen_global.meta where country_code = 'EX-US' ) gm
            );

            create temp table global as (
                select
                    gm.id as global_id,
                    q.*,
                    row_number() over (order by q.tw_streams desc) as rnk,
                    case
                        when q.lw_streams = 0 then 0
                        else round(round(q.tw_streams::numeric - q.lw_streams::numeric) / q.lw_streams::numeric * 100, 2)
                    end as pct_chg
                from (
                    select
                        coalesce(st.artist_id, ex.artist_id) as artist_id,
                        coalesce(ex.tw_streams, 0) + coalesce(st.tw_streams, 0) as tw_streams,
                        coalesce(ex.lw_streams, 0) + coalesce(st.lw_streams, 0) as lw_streams
                    from ex_us ex
                    full outer join nielsen_artist.__stats st on ex.artist_id = st.artist_id
                ) q
                cross join ( select * from nielsen_global.meta where country_code = 'GLOBAL' ) gm
            );

            insert into nielsen_artist.global_stats (global_id, artist_id, rnk, tw_streams, lw_streams, pct_chg, ytd_streams, rtd_streams, digital_song_sales_tw)
            select global_id, artist_id, rnk, tw_streams, lw_streams, pct_chg, ytd_streams, rtd_streams, digital_song_sales_tw from ex_us
            on conflict (global_id, artist_id) do update
            set
                rnk = excluded.rnk,
                tw_streams = excluded.tw_streams,
                lw_streams = excluded.lw_streams,
                pct_chg = excluded.pct_chg,
                ytd_streams = excluded.ytd_streams,
                rtd_streams = excluded.rtd_streams,
                digital_song_sales_tw = excluded.digital_song_sales_tw;

            insert into nielsen_artist.global_stats (global_id, artist_id, rnk, tw_streams, lw_streams, pct_chg, ytd_streams, rtd_streams, digital_song_sales_tw)
            select global_id, artist_id, rnk, tw_streams, lw_streams, pct_chg, 0, 0, 0 from global
            on conflict (global_id, artist_id) do update
            set
                rnk = excluded.rnk,
                tw_streams = excluded.tw_streams,
                lw_streams = excluded.lw_streams,
                pct_chg = excluded.pct_chg,
                ytd_streams = excluded.ytd_streams,
                rtd_streams = excluded.rtd_streams,
                digital_song_sales_tw = excluded.digital_song_sales_tw;

            drop table ex_us;
            drop table global;

            create temp table ex_us as (
                select
                    gm.id as global_id,
                    q.*,
                    row_number() over (order by q.tw_streams desc) as rnk,
                    case
                        when q.lw_streams = 0 then 0
                        else round(round(q.tw_streams::numeric - q.lw_streams::numeric) / q.lw_streams::numeric * 100, 2)
                    end as pct_chg
                from (
                    select
                        song_id,
                        sum(tw_streams) as tw_streams,
                        sum(lw_streams) as lw_streams,
                        sum(ytd_streams) as ytd_streams,
                        sum(rtd_streams) as rtd_streams,
                        sum(digital_song_sales_tw) as digital_song_sales_tw
                    from nielsen_song.global_stats
                    where global_id not in (67, 68)
                    group by song_id
                ) q
                cross join ( select * from nielsen_global.meta where country_code = 'EX-US' ) gm
            );

            create temp table global as (
                select
                    gm.id as global_id,
                    q.*,
                    row_number() over (order by q.tw_streams desc) as rnk,
                    case
                        when q.lw_streams = 0 then 0
                        else round(round(q.tw_streams::numeric - q.lw_streams::numeric) / q.lw_streams::numeric * 100, 2)
                    end as pct_chg
                from (
                    select
                        coalesce(st.song_id, ex.song_id) as song_id,
                        coalesce(ex.tw_streams, 0) + coalesce(st.tw_streams, 0) as tw_streams,
                        coalesce(ex.lw_streams, 0) + coalesce(st.lw_streams, 0) as lw_streams
                    from ex_us ex
                    full outer join nielsen_song.__stats st on ex.song_id = st.song_id
                ) q
                cross join ( select * from nielsen_global.meta where country_code = 'GLOBAL' ) gm
            );

            insert into nielsen_song.global_stats (global_id, song_id, rnk, tw_streams, lw_streams, pct_chg, ytd_streams, rtd_streams, digital_song_sales_tw)
            select global_id, song_id, rnk, tw_streams, lw_streams, pct_chg, ytd_streams, rtd_streams, digital_song_sales_tw from ex_us
            on conflict (global_id, song_id) do update
            set
                rnk = excluded.rnk,
                tw_streams = excluded.tw_streams,
                lw_streams = excluded.lw_streams,
                pct_chg = excluded.pct_chg,
                ytd_streams = excluded.ytd_streams,
                rtd_streams = excluded.rtd_streams,
                digital_song_sales_tw = excluded.digital_song_sales_tw;

            insert into nielsen_song.global_stats (global_id, song_id, rnk, tw_streams, lw_streams, pct_chg, ytd_streams, rtd_streams, digital_song_sales_tw)
            select global_id, song_id, rnk, tw_streams, lw_streams, pct_chg, 0, 0, 0 from global
            on conflict (global_id, song_id) do update
            set
                rnk = excluded.rnk,
                tw_streams = excluded.tw_streams,
                lw_streams = excluded.lw_streams,
                pct_chg = excluded.pct_chg,
                ytd_streams = excluded.ytd_streams,
                rtd_streams = excluded.rtd_streams,
                digital_song_sales_tw = excluded.digital_song_sales_tw;
        """
        self.db.execute(string)
    
    def updateGlobalAggregates(self):

        string = """
            with data as (
                select
                    q.*,
                    case
                        when q.lw_streams = 0 then 0
                        else round(round(q.tw_streams::numeric - q.lw_streams::numeric) / q.lw_streams::numeric * 100, 2)
                    end as pct_chg,
                    row_number() over (order by tw_streams desc) as rnk
                from (
                    select
                        global_id,
                        sum(tw_streams) as tw_streams,
                        sum(lw_streams) as lw_streams,
                        sum(ytd_streams) as ytd_streams,
                        sum(rtd_streams) as rtd_streams
                    from nielsen_artist.global_stats
                    group by global_id
                ) q
            )

            update nielsen_global.meta m
            set
                tw_streams = data.tw_streams,
                lw_streams = data.lw_streams,
                pct_chg = data.pct_chg,
                ytd_streams = data.ytd_streams,
                rtd_streams = data.rtd_streams,
                rnk = data.rnk
            from data
            where m.id = data.global_id;
        """
        self.db.execute(string)
    
    def updateGlobalCorrelations(self):

        string = """
            with territory_streams as (
                select
                    global_id,
                    artist_id,
                    tw_streams
                from nielsen_artist.global_stats
            ), territory_artist_counts as (
                select
                    global_id,
                    count(distinct artist_id) as total_artists
                from territory_streams
                group by global_id
            ), shared_artists as (
                select
                    t1.global_id,
                    t2.global_id as c_global_id,
                    count(distinct t1.artist_id) as shared_artists_count
                from territory_streams t1
                join territory_streams t2 on t1.artist_id = t2.artist_id and t1.global_id <> t2.global_id
                group by t1.global_id, t2.global_id
            ), correlations as (
                select
                    s.global_id,
                    s.c_global_id,
                    round(100.0 * s.shared_artists_count / least(c1.total_artists, c2.total_artists)) as correlation,
                    s.shared_artists_count as instance_count
                from shared_artists s
                join territory_artist_counts c1 on s.global_id = c1.global_id
                join territory_artist_counts c2 on s.c_global_id = c2.global_id
                order by s.global_id, s.c_global_id
            )

            insert into nielsen_global.correlations (global_id, c_global_id, correlation, instance_count)
            select global_id, c_global_id, correlation, instance_count from correlations
            on conflict (global_id, c_global_id) do update
            set
                correlation = excluded.correlation,
                instance_count = excluded.instance_count;
        """
        self.db.execute(string)
    
    def build(self):
        
        files = self.getNewWeeklyFiles()
        print(f'Processing {len(files)} new files')

        files = files.sort_values(by='server_name').reset_index(drop=True).to_dict('records')

        for file in files:
            self.addProcessFunc(file)

        self.add_function(self.updateGlobalAndExUs, 'Update Global & Ex US')
        self.add_function(self.updateGlobalAggregates, 'Update Global Aggregated Stats')
        
        # If we processed new global files, we need to update the correlations graph
        if len(files) > 0:
            self.add_function(self.updateGlobalCorrelations, 'Update Global Correlations')

    def test_build(self):
        pass