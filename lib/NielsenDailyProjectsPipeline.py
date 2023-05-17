from .PipelineBase import PipelineBase


class NielsenDailyProjectsPipeline(PipelineBase):

    """
        I know it seems crazy to have an entire pipeline for something that seems so simple,
        but this is the only way to ensure that the project data is updated correctly.
        The project data should be updated every day, but on monday when we update the
        mapping table, we need to make sure that the project data is updated after that happens.
        Therefore, we need to separate the project pipeline out from the main pipeline, so that
        we can run it after the main pipeline and the mapping table (when that happens) have been updated.
    """

    def __init__(self, db_name):
        PipelineBase.__init__(self, db_name)

    def updateProjectMeta(self):

        string = """
            insert into nielsen_project.meta (unified_collection_id, artist_id, name, release_date, track_count, is_valid, type)
            select unified_collection_id, artist_id, name, release_date, track_count, is_valid, type
            from (
                select
                    unified_collection_id, artist_id, name, release_date, track_count, type,
                    case
                        when
                                num_artist_tracks > 4 and
                                confidence > 0.5 and
                                type = 'Album' and
                                name !~* '.*best of.*' and
                                name !~* '.*the greatest.*' and
                                name !~* '.*the best.*'
                            then true
                        else false
                    end as is_valid
                from (
                    select
                        unified_collection_id, artist_id, name, release_date, track_count, num_artist_tracks, num_artist_tracks_rank, type,
                        round(num_artist_tracks / track_count, 1) as confidence
                    from (
                        select unified_collection_id, artist_id, name, release_date, num_artist_tracks, num_artist_tracks_rank, type,
                            sum(num_artist_tracks) over (partition by unified_collection_id) as track_count
                        from (
                            select
                                c.unified_collection_id,
                                m.id as artist_id,
                                c.name,
                                c.release_date,
                                c.type,
                                count(map.unified_artist_id) as num_artist_tracks,
                                row_number() over (partition by c.unified_collection_id order by count(m.id) desc nulls last) as num_artist_tracks_rank
                            from nielsen_map.collections c
                            left join nielsen_map.map on c.unified_collection_id = map.unified_collection_id
                            join nielsen_artist.meta m on map.unified_artist_id = m.unified_artist_id
                            group by c.unified_collection_id, m.id, c.name, c.release_date, c.type
                        ) q
                    ) q
                ) q
                where num_artist_tracks_rank = 1
            ) q
            on conflict (unified_collection_id) do update
            set
                name = excluded.name,
                release_date = excluded.release_date,
                track_count = excluded.track_count,
                is_valid = excluded.is_valid,
                artist_id = excluded.artist_id
        """
        self.db.execute(string)

    def updateProjectImages(self):

        string = """
            update nielsen_project.meta m
            set
                spotify_image = q.spotify_image,
                dominant_color = q.dominant_color
            from (
                select project_id, dominant_color, spotify_image
                from (
                    select *, row_number() over (partition by project_id order by image_instance_count desc nulls last) as img_rank
                    from (
                        select
                            m.id as project_id,
                            m.dominant_color,
                            sp.spotify_image,
                            count(sp.spotify_image) as image_instance_count
                        from nielsen_project.meta m
                        join nielsen_map.map on m.unified_collection_id = map.unified_collection_id
                        join nielsen_song.meta sm on map.unified_song_id = sm.unified_song_id
                        join nielsen_song.spotify sp on sm.id = sp.song_id
                        where m.spotify_image is null
                            and sp.spotify_image is not null
                            and sp.spotify_image != ''
                        group by m.id, m.dominant_color, sp.spotify_image
                    ) q
                ) q
                where img_rank = 1
            ) q
            where m.id = q.project_id
        """
        self.db.execute(string)

    def updateProjectStreams(self):

        string = """
            insert into nielsen_project.streams (project_id, date, streams)
            select
                m.id as project_id,
                s.date,
                sum(coalesce(s.streams, 0)) as streams
            from nielsen_project.meta m
            join nielsen_song.track_collections tc on m.unified_collection_id = tc.unified_collection_id
            join nielsen_song.streams s on tc.song_id = s.song_id
            where s.date > current_date - interval '10 days'
            group by m.id, s.date
            on conflict (project_id, date) do update
            set streams = excluded.streams
        """
        self.db.execute(string)

    def updateProjectWeeklyMeta(self):

        string = """
            update nielsen_project.meta m
            set
                tw_streams = q.tw_streams,
                lw_streams = q.lw_streams,
                rtd_oda_streams = q.rtd_oda_streams,
                est_revenue_per_year = q.est_revenue_per_year,
                est_total_revenue = q.est_total_revenue,
                pct_chg = q.pct_chg
            from (
                select
                    project_id,
                    tw_streams,
                    lw_streams,
                    rtd_oda_streams,
                    est_revenue_per_year,
                    est_total_revenue,
                    case
                        when lw_streams = 0 then 0
                        else round((tw_streams - lw_streams) / lw_streams, 2)
                    end as pct_chg
                from (
                    select
                        m.id as project_id,
                        sum(coalesce(st.tw_streams, 0)) as tw_streams,
                        sum(coalesce(st.lw_streams, 0)) as lw_streams,
                        sum(coalesce(s.rtd_oda_streams, 0)) as rtd_oda_streams,
                        sum(coalesce(st.tw_streams, 0) * 52 * 0.0056) as est_revenue_per_year,
                        sum(coalesce(s.rtd_oda_streams, 0) * 0.0056) as est_total_revenue
                    from nielsen_project.meta m
                    join nielsen_song.track_collections tc on m.unified_collection_id = tc.unified_collection_id
                    join nielsen_song.__stats st on tc.song_id = st.song_id
                    join nielsen_song.__song s on tc.song_id = s.song_id
                    group by m.id
                ) q
            ) q
            where m.id = q.project_id
        """
        self.db.execute(string)

    def build(self):

        """
            These functions don't technically need to be separated into their own functions, but it's
            just slightly easier read this way.

            Their order does matter, however, so be careful when changing the order of the functions.
            For info on their dependency order, see the docstrings of the functions themselves.
        """
        
        """
            Step 1:
                Update the project meta table by inserting any new projects and updating any existing projects.
                This function must be run first in order for the other functions to capture new projects.
        """
        self.add_function(self.updateProjectMeta, 'Update Project Meta')

        """
            Step 2:
                - updateProjectImages
                    = Gather the most common image for each project from the Spotify table.
                    = Also gather the dominant color for each image.
                    = Depends on the project meta table being up to date.
                - updateProjectStreams
                    = Gather the streams for each project from the Streams table and save as aggregated project streams
                    = Depends on the project meta table being up to date.
                - updateProjectWeeklyMeta
                    = Gather the weekly streams for each project from the Streams table and save as aggregated project streams
                    = Depends on the project meta table being up to date.
        """
        self.add_function(self.updateProjectImages, 'Update Project Images')
        self.add_function(self.updateProjectStreams, 'Update Project Streams')
        self.add_function(self.updateProjectWeeklyMeta, 'Update Project Weekly Meta')

    def test_build(self):
        
        self.add_function(self.updateProjectMeta, 'Update Project Meta')
        self.add_function(self.updateProjectImages, 'Update Project Images')
        self.add_function(self.updateProjectStreams, 'Update Project Streams')
        self.add_function(self.updateProjectWeeklyMeta, 'Update Project Weekly Meta')