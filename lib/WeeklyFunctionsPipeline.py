from .functions import chunker
from .PipelineBase import PipelineBase
from .Db import Db
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

class WeeklyFunctionsPipeline(PipelineBase):

    def __init__(self, db_name):
        PipelineBase.__init__(self, db_name)

    def cacheChartmetricIds(self):

        # Start by getting the spotify artist ids from our db
        string = """
            select
                artist_id,
                unified_artist_id,
                spotify_artist_id
            from nielsen_artist.cm_map
            where spotify_artist_id is not null
                and length(spotify_artist_id) > 0
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

    def cacheProjectReports(self):

        string = """
            -- Projects
            delete from nielsen_project.reports_recent;

            with base as (
                select
                    r.song_id,
                    r.rtd_oda_streams,
                    r.tw_oda_streams,
                    m.unified_song_id,
                    m.core_genre,
                    map.unified_collection_id,
                    map.unified_artist_id,
                    am.id as artist_id,
                    am.artist,
                    c.release_date,
                    c.name,
                    sp.spotify_image,
                    count(am.id) over (partition by map.unified_collection_id, am.id) as artist_rank,
                    count(core_genre) over (partition by map.unified_collection_id, core_genre) as genre_rank,
                    count(spotify_image) over (partition by map.unified_collection_id, spotify_image) as image_rank,
                    count(map.unified_artist_id) over (partition by map.unified_collection_id, map.unified_artist_id)::float / count(*) over (partition by map.unified_collection_id)::float as confidence
                from nielsen_song.__reports_recent r
                join nielsen_song.meta m on m.id = r.song_id
                join nielsen_map.map on map.unified_song_id = m.unified_song_id
                join nielsen_artist.meta am on am.unified_artist_id = map.unified_artist_id
                join nielsen_map.collections c on c.unified_collection_id = map.unified_collection_id
                join nielsen_song.spotify sp on sp.song_id = r.song_id
                where map.unified_collection_id is not null
                    and map.unified_artist_id is not null
                    and c.type = 'Album'
                    and c.name !~* '.*best of.*'
                    and c.name !~* '.*the greatest.*'
                    and c.name !~* '.*the best.*'
            ), genres as (
                select distinct on (unified_collection_id) unified_collection_id, core_genre
                from base
                group by unified_collection_id, core_genre, genre_rank
                order by unified_collection_id, genre_rank desc
            ), artists as (
                select distinct on (unified_collection_id) unified_collection_id, artist_id, artist
                from base
                group by unified_collection_id, artist_id, artist, artist_rank
                order by unified_collection_id, artist_rank desc
            ), images as (
                select distinct on (unified_collection_id) unified_collection_id, spotify_image
                from base
                group by unified_collection_id, spotify_image, image_rank
                order by unified_collection_id, image_rank desc
            ), results as (
                select
                    b.unified_collection_id,
                    sum(rtd_oda_streams) as rtd_oda_streams,
                    sum(tw_oda_streams) as tw_oda_streams,
                    name,
                    b.release_date,
                    g.core_genre,
                    a.artist_id,
                    a.artist,
                    i.spotify_image
                from base b
                join genres g on b.unified_collection_id = g.unified_collection_id
                join artists a on b.unified_collection_id = a.unified_collection_id
                join images i on b.unified_collection_id = i.unified_collection_id
                group by b.unified_collection_id, b.release_date, name, g.core_genre, a.artist_id, a.artist, i.spotify_image
                having count(b.unified_collection_id) > 4 and max(confidence) > 0.5
            )

            insert into nielsen_project.reports_recent (
                artist_id,
                unified_collection_id,
                rtd_oda_streams,
                tw_streams,
                name,
                release_date,
                core_genre,
                artist,
                spotify_image
            )
            select
                artist_id,
                unified_collection_id,
                rtd_oda_streams,
                tw_oda_streams,
                name,
                release_date,
                core_genre,
                artist,
                spotify_image
            from results;
        """
        self.db.execute(string)

    def build(self):
        
        self.add_function(self.cacheChartmetricIds, 'Recache Chartmetric Ids')
        self.add_function(self.cacheProjectReports, 'Cache Project Reports')

    def test_build(self):
        pass