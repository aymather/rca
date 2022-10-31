from rca import Db
from .PipelineBase import PipelineBase

def cacheChartmetricIds(settings, db):

    # Start by getting the spotify artist ids from our db
    string = """
        select *
        from nielsen_artist.cm_map
        where spotify_artist_id is not null
            and length(spotify_artist_id) > 0
    """
    df = db.execute(string)

    spotify_artist_ids = tuple(df['spotify_artist_id'].unique())

    reporting_db = Db('reporting_db')
    reporting_db.connect()

    # Get the charmetric mapping
    string = """
        with temp as (
            select
                cm_artist as target_id,
                spotify_artist_id as spotify_id
            from chartmetric_raw.spotify_artist
            where spotify_artist_id in %(spotify_ids)s
        ), t as (
            select
                temp.*, cm.account_id, cm.type
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
        group by t.target_id, t.spotify_id, sa.id, instagram_id, youtube_id, tiktok_id, shazam_id, twitter_id, genius_id, gtrends_id, soundcloud_id, twitch_id
    """
    params = { 'spotify_ids': spotify_artist_ids }
    data = reporting_db.execute(string, params)

    # Disconnect from reporting db
    reporting_db.disconnect()

    # Attach the new ids to the existing artist_id/spotify_artist_id
    data = pd.merge(data, df[['artist_id', 'spotify_artist_id']], on='spotify_artist_id', how='inner')

    # Remove existing records
    string = """
        delete
        from nielsen_artist.cm_map
        where artist_id in %(artist_ids)s
    """
    params = { 'artist_ids': tuple(data.artist_id.values) }

    # Insert updated records
    db.big_insert(data, 'nielsen_artist.cm_map')

def cacheArtistDiscoveryStats(settings, db):

    string = """
        -- Cache daily artist streams for discovery
        delete from nielsen_artist.daily_streams_cache;

        with streams as (
            select
                artist_id,
                date,
                streams,
                (
                case
                    when prev is null or prev = 0 then 0
                    when round(100 * (streams - prev)::numeric / prev, 2) < -50 then -50
                    when round(100 * (streams - prev)::numeric / prev, 2) > 50 then 50
                    else round(100 * (streams - prev)::numeric / prev, 2)
                end
                ) as pct_chg
            from (
                select
                artist_id,
                streams,
                date,
                lag(streams, 1) over (partition by artist_id order by date) as prev
                from nielsen_artist.streams
                where date >= now() - interval '6 months'
                order by artist_id, date desc
            ) q
        ), tw_averages as (
            select
                artist_id,
                tw_streams_avg,
                tw_streams_rpc,
                (
                case
                    when tw_streams_avg <= 1428 then 1
                    when tw_streams_avg > 1428 and tw_streams_avg <= 2857 then 2
                    when tw_streams_avg > 2857 and tw_streams_avg <= 5714 then 3
                    when tw_streams_avg > 5714 and tw_streams_avg <= 14285 then 4
                    when tw_streams_avg > 14285 and tw_streams_avg <= 35714 then 5
                    when tw_streams_avg > 35714 and tw_streams_avg <= 71428 then 6
                    when tw_streams_avg > 71428 and tw_streams_avg <= 142857 then 7
                    when tw_streams_avg > 142857 and tw_streams_avg <= 357142 then 8
                    when tw_streams_avg > 357142 then 9
                end
                ) as tw_streams_tier
            from (
                select
                artist_id,
                round(avg(streams)) as tw_streams_avg,
                round(avg(pct_chg), 2) as tw_streams_rpc
                from streams
                where date >= now() - interval '10 days'
                group by artist_id
                having count(artist_id) > 5
            ) q
        ), tm_averages as (
            select
                artist_id,
                tm_streams_avg,
                tm_streams_rpc,
                (
                case
                    when tm_streams_avg <= 1428 then 1
                    when tm_streams_avg > 1428 and tm_streams_avg <= 2857 then 2
                    when tm_streams_avg > 2857 and tm_streams_avg <= 5714 then 3
                    when tm_streams_avg > 5714 and tm_streams_avg <= 14285 then 4
                    when tm_streams_avg > 14285 and tm_streams_avg <= 35714 then 5
                    when tm_streams_avg > 35714 and tm_streams_avg <= 71428 then 6
                    when tm_streams_avg > 71428 and tm_streams_avg <= 142857 then 7
                    when tm_streams_avg > 142857 and tm_streams_avg <= 357142 then 8
                    when tm_streams_avg > 357142 then 9
                end
                ) as tm_streams_tier
            from (
                select
                artist_id,
                round(avg(streams)) as tm_streams_avg,
                round(avg(pct_chg), 2) as tm_streams_rpc
                from streams
                where date >= now() - interval '32 days'
                group by artist_id
                having count(artist_id) > 23
            ) q
        ), sm_averages as (
            select
                artist_id,
                sm_streams_avg,
                sm_streams_rpc,
                (
                case
                    when sm_streams_avg <= 1428 then 1
                    when sm_streams_avg > 1428 and sm_streams_avg <= 2857 then 2
                    when sm_streams_avg > 2857 and sm_streams_avg <= 5714 then 3
                    when sm_streams_avg > 5714 and sm_streams_avg <= 14285 then 4
                    when sm_streams_avg > 14285 and sm_streams_avg <= 35714 then 5
                    when sm_streams_avg > 35714 and sm_streams_avg <= 71428 then 6
                    when sm_streams_avg > 71428 and sm_streams_avg <= 142857 then 7
                    when sm_streams_avg > 142857 and sm_streams_avg <= 357142 then 8
                    when sm_streams_avg > 357142 then 9
                end
                ) as sm_streams_tier
            from (
                select
                artist_id,
                round(avg(streams)) as sm_streams_avg,
                round(avg(pct_chg), 2) as sm_streams_rpc
                from streams
                group by artist_id
                having count(artist_id) > 120
            ) q
        ), results as (
            select
                r.artist_id,
                twa.tw_streams_avg,
                twa.tw_streams_rpc,
                twa.tw_streams_tier,
                tma.tm_streams_avg,
                tma.tm_streams_rpc,
                tma.tm_streams_tier,
                sma.sm_streams_avg,
                sma.sm_streams_rpc,
                sma.sm_streams_tier,
                r.signed
            from nielsen_artist.reports_recent r
            left join tw_averages twa on r.artist_id = twa.artist_id
            left join tm_averages tma on r.artist_id = tma.artist_id
            left join sm_averages sma on r.artist_id = sma.artist_id
            ), p as (
            select
                avg(tw_streams_rpc) + stddev(tw_streams_rpc) as tw_upper,
                avg(tw_streams_rpc) - stddev(tw_streams_rpc) as tw_lower,
                avg(tw_streams_rpc) as tw_avg,
                avg(tm_streams_rpc) + stddev(tm_streams_rpc) as tm_upper,
                avg(tm_streams_rpc) - stddev(tm_streams_rpc) as tm_lower,
                avg(tm_streams_rpc) as tm_avg,
                avg(sm_streams_rpc) + stddev(sm_streams_rpc) as sm_upper,
                avg(sm_streams_rpc) - stddev(sm_streams_rpc) as sm_lower,
                avg(sm_streams_rpc) as sm_avg
            from results
        ), final as (
            select
                *,
                (
                case
                    when tw_streams_rpc < (select tw_lower from p) then 1
                    when tw_streams_rpc >= (select tw_lower from p) and tw_streams_rpc < (select tw_avg from p) then 2
                    when tw_streams_rpc >= (select tw_avg from p) and tw_streams_rpc < (select tw_upper from p) then 3
                    when tw_streams_rpc >= (select tw_upper from p) then 4
                    else 5
                end
                ) as tw_streams_status,
                (
                case
                    when tm_streams_rpc < (select tm_lower from p) then 1
                    when tm_streams_rpc >= (select tm_lower from p) and tm_streams_rpc < (select tm_avg from p) then 2
                    when tm_streams_rpc >= (select tm_avg from p) and tm_streams_rpc < (select tm_upper from p) then 3
                    when tm_streams_rpc >= (select tm_upper from p) then 4
                    else 5
                end
                ) as tm_streams_status,
                (
                case
                    when sm_streams_rpc < (select sm_lower from p) then 1
                    when sm_streams_rpc >= (select sm_lower from p) and sm_streams_rpc < (select sm_avg from p) then 2
                    when sm_streams_rpc >= (select sm_avg from p) and sm_streams_rpc < (select sm_upper from p) then 3
                    when sm_streams_rpc >= (select sm_upper from p) then 4
                    else 5
                end
                ) as sm_streams_status
            from results
        )

        insert into nielsen_artist.daily_streams_cache (
            artist_id,
            tw_streams_avg, tw_streams_rpc, tw_streams_tier, tw_streams_status,
            tm_streams_avg, tm_streams_rpc, tm_streams_tier, tm_streams_status,
            sm_streams_avg, sm_streams_rpc, sm_streams_tier, sm_streams_status,
            signed
        )
        select
            artist_id,
            tw_streams_avg, tw_streams_rpc, tw_streams_tier, tw_streams_status,
            tm_streams_avg, tm_streams_rpc, tm_streams_tier, tm_streams_status,
            sm_streams_avg, sm_streams_rpc, sm_streams_tier, sm_streams_status,
            signed
        from final;

        -- Cache daily song streams for discovery
        delete from nielsen_song.daily_streams_cache;

        with streams as (
            select
                song_id,
                date,
                streams,
                (
                case
                    when prev is null or prev = 0 then 0
                    when round(100 * (streams - prev)::numeric / prev, 2) < -50 then -50
                    when round(100 * (streams - prev)::numeric / prev, 2) > 50 then 50
                    else round(100 * (streams - prev)::numeric / prev, 2)
                end
                ) as pct_chg
            from (
                select
                song_id,
                streams,
                date,
                lag(streams, 1) over (partition by song_id order by date) as prev
                from nielsen_song.streams
                where date >= now() - interval '6 months'
                order by song_id, date desc
            ) q
        ), tw_averages as (
            select
                song_id,
                tw_streams_avg,
                tw_streams_rpc,
                (
                case
                    when tw_streams_avg <= 1428 then 1
                    when tw_streams_avg > 1428 and tw_streams_avg <= 2857 then 2
                    when tw_streams_avg > 2857 and tw_streams_avg <= 5714 then 3
                    when tw_streams_avg > 5714 and tw_streams_avg <= 14285 then 4
                    when tw_streams_avg > 14285 and tw_streams_avg <= 35714 then 5
                    when tw_streams_avg > 35714 and tw_streams_avg <= 71428 then 6
                    when tw_streams_avg > 71428 and tw_streams_avg <= 142857 then 7
                    when tw_streams_avg > 142857 and tw_streams_avg <= 357142 then 8
                    when tw_streams_avg > 357142 then 9
                end
                ) as tw_streams_tier
            from (
                select
                song_id,
                round(avg(streams)) as tw_streams_avg,
                round(avg(pct_chg), 2) as tw_streams_rpc
                from streams
                where date >= now() - interval '10 days'
                group by song_id
                having count(song_id) > 5
            ) q
        ), tm_averages as (
            select
                song_id,
                tm_streams_avg,
                tm_streams_rpc,
                (
                case
                    when tm_streams_avg <= 1428 then 1
                    when tm_streams_avg > 1428 and tm_streams_avg <= 2857 then 2
                    when tm_streams_avg > 2857 and tm_streams_avg <= 5714 then 3
                    when tm_streams_avg > 5714 and tm_streams_avg <= 14285 then 4
                    when tm_streams_avg > 14285 and tm_streams_avg <= 35714 then 5
                    when tm_streams_avg > 35714 and tm_streams_avg <= 71428 then 6
                    when tm_streams_avg > 71428 and tm_streams_avg <= 142857 then 7
                    when tm_streams_avg > 142857 and tm_streams_avg <= 357142 then 8
                    when tm_streams_avg > 357142 then 9
                end
                ) as tm_streams_tier
            from (
                select
                song_id,
                round(avg(streams)) as tm_streams_avg,
                round(avg(pct_chg), 2) as tm_streams_rpc
                from streams
                where date >= now() - interval '32 days'
                group by song_id
                having count(song_id) > 23
            ) q
        ), sm_averages as (
            select
                song_id,
                sm_streams_avg,
                sm_streams_rpc,
                (
                case
                    when sm_streams_avg <= 1428 then 1
                    when sm_streams_avg > 1428 and sm_streams_avg <= 2857 then 2
                    when sm_streams_avg > 2857 and sm_streams_avg <= 5714 then 3
                    when sm_streams_avg > 5714 and sm_streams_avg <= 14285 then 4
                    when sm_streams_avg > 14285 and sm_streams_avg <= 35714 then 5
                    when sm_streams_avg > 35714 and sm_streams_avg <= 71428 then 6
                    when sm_streams_avg > 71428 and sm_streams_avg <= 142857 then 7
                    when sm_streams_avg > 142857 and sm_streams_avg <= 357142 then 8
                    when sm_streams_avg > 357142 then 9
                end
                ) as sm_streams_tier
            from (
                select
                song_id,
                round(avg(streams)) as sm_streams_avg,
                round(avg(pct_chg), 2) as sm_streams_rpc
                from streams
                group by song_id
                having count(song_id) > 120
            ) q
        ), results as (
            select
                r.song_id,
                twa.tw_streams_avg,
                twa.tw_streams_rpc,
                twa.tw_streams_tier,
                tma.tm_streams_avg,
                tma.tm_streams_rpc,
                tma.tm_streams_tier,
                sma.sm_streams_avg,
                sma.sm_streams_rpc,
                sma.sm_streams_tier,
                r.signed
            from nielsen_song.reports_recent r
            left join tw_averages twa on r.song_id = twa.song_id
            left join tm_averages tma on r.song_id = tma.song_id
            left join sm_averages sma on r.song_id = sma.song_id
        ), p as (
            select
                avg(tw_streams_rpc) + stddev(tw_streams_rpc) as tw_upper,
                avg(tw_streams_rpc) - stddev(tw_streams_rpc) as tw_lower,
                avg(tw_streams_rpc) as tw_avg,
                avg(tm_streams_rpc) + stddev(tm_streams_rpc) as tm_upper,
                avg(tm_streams_rpc) - stddev(tm_streams_rpc) as tm_lower,
                avg(tm_streams_rpc) as tm_avg,
                avg(sm_streams_rpc) + stddev(sm_streams_rpc) as sm_upper,
                avg(sm_streams_rpc) - stddev(sm_streams_rpc) as sm_lower,
                avg(sm_streams_rpc) as sm_avg
            from results
            ), final as (
            select
                *,
                (
                case
                    when tw_streams_rpc < (select tw_lower from p) then 1
                    when tw_streams_rpc >= (select tw_lower from p) and tw_streams_rpc < (select tw_avg from p) then 2
                    when tw_streams_rpc >= (select tw_avg from p) and tw_streams_rpc < (select tw_upper from p) then 3
                    when tw_streams_rpc >= (select tw_upper from p) then 4
                    else 5
                end
                ) as tw_streams_status,
                (
                case
                    when tm_streams_rpc < (select tm_lower from p) then 1
                    when tm_streams_rpc >= (select tm_lower from p) and tm_streams_rpc < (select tm_avg from p) then 2
                    when tm_streams_rpc >= (select tm_avg from p) and tm_streams_rpc < (select tm_upper from p) then 3
                    when tm_streams_rpc >= (select tm_upper from p) then 4
                    else 5
                end
                ) as tm_streams_status,
                (
                case
                    when sm_streams_rpc < (select sm_lower from p) then 1
                    when sm_streams_rpc >= (select sm_lower from p) and sm_streams_rpc < (select sm_avg from p) then 2
                    when sm_streams_rpc >= (select sm_avg from p) and sm_streams_rpc < (select sm_upper from p) then 3
                    when sm_streams_rpc >= (select sm_upper from p) then 4
                    else 5
                end
                ) as sm_streams_status
            from results
        )

        insert into nielsen_song.daily_streams_cache (
            song_id,
            tw_streams_avg, tw_streams_rpc, tw_streams_tier, tw_streams_status,
            tm_streams_avg, tm_streams_rpc, tm_streams_tier, tm_streams_status,
            sm_streams_avg, sm_streams_rpc, sm_streams_tier, sm_streams_status,
            signed
        )
        select
            song_id,
            tw_streams_avg, tw_streams_rpc, tw_streams_tier, tw_streams_status,
            tm_streams_avg, tm_streams_rpc, tm_streams_tier, tm_streams_status,
            sm_streams_avg, sm_streams_rpc, sm_streams_tier, sm_streams_status,
            signed
        from final;
    """
    db.execute(string)

def cacheStreamingStats(settings, db):

    string = """
        -- Cache daily artist streams for discovery
        delete from nielsen_artist.daily_streams_cache;

        with streams as (
        select
            artist_id,
            date,
            streams,
            (
            case
                when prev is null or prev = 0 then 0
                when round(100 * (streams - prev)::numeric / prev, 2) < -50 then -50
                when round(100 * (streams - prev)::numeric / prev, 2) > 50 then 50
                else round(100 * (streams - prev)::numeric / prev, 2)
            end
            ) as pct_chg
        from (
            select
            artist_id,
            streams,
            date,
            lag(streams, 1) over (partition by artist_id order by date) as prev
            from nielsen_artist.streams
            where date >= now() - interval '6 months'
            order by artist_id, date desc
        ) q
        ), tw_averages as (
        select
            artist_id,
            tw_streams_avg,
            tw_streams_rpc,
            (
            case
                when tw_streams_avg <= 1428 then 1
                when tw_streams_avg > 1428 and tw_streams_avg <= 2857 then 2
                when tw_streams_avg > 2857 and tw_streams_avg <= 5714 then 3
                when tw_streams_avg > 5714 and tw_streams_avg <= 14285 then 4
                when tw_streams_avg > 14285 and tw_streams_avg <= 35714 then 5
                when tw_streams_avg > 35714 and tw_streams_avg <= 71428 then 6
                when tw_streams_avg > 71428 and tw_streams_avg <= 142857 then 7
                when tw_streams_avg > 142857 and tw_streams_avg <= 357142 then 8
                when tw_streams_avg > 357142 then 9
            end
            ) as tw_streams_tier
        from (
            select
            artist_id,
            round(avg(streams)) as tw_streams_avg,
            round(avg(pct_chg), 2) as tw_streams_rpc
            from streams
            where date >= now() - interval '10 days'
            group by artist_id
            having count(artist_id) > 5
        ) q
        ), tm_averages as (
        select
            artist_id,
            tm_streams_avg,
            tm_streams_rpc,
            (
            case
                when tm_streams_avg <= 1428 then 1
                when tm_streams_avg > 1428 and tm_streams_avg <= 2857 then 2
                when tm_streams_avg > 2857 and tm_streams_avg <= 5714 then 3
                when tm_streams_avg > 5714 and tm_streams_avg <= 14285 then 4
                when tm_streams_avg > 14285 and tm_streams_avg <= 35714 then 5
                when tm_streams_avg > 35714 and tm_streams_avg <= 71428 then 6
                when tm_streams_avg > 71428 and tm_streams_avg <= 142857 then 7
                when tm_streams_avg > 142857 and tm_streams_avg <= 357142 then 8
                when tm_streams_avg > 357142 then 9
            end
            ) as tm_streams_tier
        from (
            select
            artist_id,
            round(avg(streams)) as tm_streams_avg,
            round(avg(pct_chg), 2) as tm_streams_rpc
            from streams
            where date >= now() - interval '32 days'
            group by artist_id
            having count(artist_id) > 23
        ) q
        ), sm_averages as (
        select
            artist_id,
            sm_streams_avg,
            sm_streams_rpc,
            (
            case
                when sm_streams_avg <= 1428 then 1
                when sm_streams_avg > 1428 and sm_streams_avg <= 2857 then 2
                when sm_streams_avg > 2857 and sm_streams_avg <= 5714 then 3
                when sm_streams_avg > 5714 and sm_streams_avg <= 14285 then 4
                when sm_streams_avg > 14285 and sm_streams_avg <= 35714 then 5
                when sm_streams_avg > 35714 and sm_streams_avg <= 71428 then 6
                when sm_streams_avg > 71428 and sm_streams_avg <= 142857 then 7
                when sm_streams_avg > 142857 and sm_streams_avg <= 357142 then 8
                when sm_streams_avg > 357142 then 9
            end
            ) as sm_streams_tier
        from (
            select
            artist_id,
            round(avg(streams)) as sm_streams_avg,
            round(avg(pct_chg), 2) as sm_streams_rpc
            from streams
            group by artist_id
            having count(artist_id) > 120
        ) q
        ), results as (
        select
            r.artist_id,
            twa.tw_streams_avg,
            twa.tw_streams_rpc,
            twa.tw_streams_tier,
            tma.tm_streams_avg,
            tma.tm_streams_rpc,
            tma.tm_streams_tier,
            sma.sm_streams_avg,
            sma.sm_streams_rpc,
            sma.sm_streams_tier,
            r.signed
        from nielsen_artist.reports_recent r
        left join tw_averages twa on r.artist_id = twa.artist_id
        left join tm_averages tma on r.artist_id = tma.artist_id
        left join sm_averages sma on r.artist_id = sma.artist_id
        ), p as (
        select
            avg(tw_streams_rpc) + stddev(tw_streams_rpc) as tw_upper,
            avg(tw_streams_rpc) - stddev(tw_streams_rpc) as tw_lower,
            avg(tw_streams_rpc) as tw_avg,
            avg(tm_streams_rpc) + stddev(tm_streams_rpc) as tm_upper,
            avg(tm_streams_rpc) - stddev(tm_streams_rpc) as tm_lower,
            avg(tm_streams_rpc) as tm_avg,
            avg(sm_streams_rpc) + stddev(sm_streams_rpc) as sm_upper,
            avg(sm_streams_rpc) - stddev(sm_streams_rpc) as sm_lower,
            avg(sm_streams_rpc) as sm_avg
        from results
        ), final as (
        select
            *,
            (
            case
                when tw_streams_rpc < (select tw_lower from p) then 1
                when tw_streams_rpc >= (select tw_lower from p) and tw_streams_rpc < (select tw_avg from p) then 2
                when tw_streams_rpc >= (select tw_avg from p) and tw_streams_rpc < (select tw_upper from p) then 3
                when tw_streams_rpc >= (select tw_upper from p) then 4
                else 5
            end
            ) as tw_streams_status,
            (
            case
                when tm_streams_rpc < (select tm_lower from p) then 1
                when tm_streams_rpc >= (select tm_lower from p) and tm_streams_rpc < (select tm_avg from p) then 2
                when tm_streams_rpc >= (select tm_avg from p) and tm_streams_rpc < (select tm_upper from p) then 3
                when tm_streams_rpc >= (select tm_upper from p) then 4
                else 5
            end
            ) as tm_streams_status,
            (
            case
                when sm_streams_rpc < (select sm_lower from p) then 1
                when sm_streams_rpc >= (select sm_lower from p) and sm_streams_rpc < (select sm_avg from p) then 2
                when sm_streams_rpc >= (select sm_avg from p) and sm_streams_rpc < (select sm_upper from p) then 3
                when sm_streams_rpc >= (select sm_upper from p) then 4
                else 5
            end
            ) as sm_streams_status
        from results
        )

        insert into nielsen_artist.daily_streams_cache (
        artist_id,
        tw_streams_avg, tw_streams_rpc, tw_streams_tier, tw_streams_status,
        tm_streams_avg, tm_streams_rpc, tm_streams_tier, tm_streams_status,
        sm_streams_avg, sm_streams_rpc, sm_streams_tier, sm_streams_status,
        signed
        )
        select
        artist_id,
        tw_streams_avg, tw_streams_rpc, tw_streams_tier, tw_streams_status,
        tm_streams_avg, tm_streams_rpc, tm_streams_tier, tm_streams_status,
        sm_streams_avg, sm_streams_rpc, sm_streams_tier, sm_streams_status,
        signed
        from final;

        -- Cache daily song streams for discovery
        delete from nielsen_song.daily_streams_cache;

        with streams as (
        select
            song_id,
            date,
            streams,
            (
            case
                when prev is null or prev = 0 then 0
                when round(100 * (streams - prev)::numeric / prev, 2) < -50 then -50
                when round(100 * (streams - prev)::numeric / prev, 2) > 50 then 50
                else round(100 * (streams - prev)::numeric / prev, 2)
            end
            ) as pct_chg
        from (
            select
            song_id,
            streams,
            date,
            lag(streams, 1) over (partition by song_id order by date) as prev
            from nielsen_song.streams
            where date >= now() - interval '6 months'
            order by song_id, date desc
        ) q
        ), tw_averages as (
        select
            song_id,
            tw_streams_avg,
            tw_streams_rpc,
            (
            case
                when tw_streams_avg <= 1428 then 1
                when tw_streams_avg > 1428 and tw_streams_avg <= 2857 then 2
                when tw_streams_avg > 2857 and tw_streams_avg <= 5714 then 3
                when tw_streams_avg > 5714 and tw_streams_avg <= 14285 then 4
                when tw_streams_avg > 14285 and tw_streams_avg <= 35714 then 5
                when tw_streams_avg > 35714 and tw_streams_avg <= 71428 then 6
                when tw_streams_avg > 71428 and tw_streams_avg <= 142857 then 7
                when tw_streams_avg > 142857 and tw_streams_avg <= 357142 then 8
                when tw_streams_avg > 357142 then 9
            end
            ) as tw_streams_tier
        from (
            select
            song_id,
            round(avg(streams)) as tw_streams_avg,
            round(avg(pct_chg), 2) as tw_streams_rpc
            from streams
            where date >= now() - interval '10 days'
            group by song_id
            having count(song_id) > 5
        ) q
        ), tm_averages as (
        select
            song_id,
            tm_streams_avg,
            tm_streams_rpc,
            (
            case
                when tm_streams_avg <= 1428 then 1
                when tm_streams_avg > 1428 and tm_streams_avg <= 2857 then 2
                when tm_streams_avg > 2857 and tm_streams_avg <= 5714 then 3
                when tm_streams_avg > 5714 and tm_streams_avg <= 14285 then 4
                when tm_streams_avg > 14285 and tm_streams_avg <= 35714 then 5
                when tm_streams_avg > 35714 and tm_streams_avg <= 71428 then 6
                when tm_streams_avg > 71428 and tm_streams_avg <= 142857 then 7
                when tm_streams_avg > 142857 and tm_streams_avg <= 357142 then 8
                when tm_streams_avg > 357142 then 9
            end
            ) as tm_streams_tier
        from (
            select
            song_id,
            round(avg(streams)) as tm_streams_avg,
            round(avg(pct_chg), 2) as tm_streams_rpc
            from streams
            where date >= now() - interval '32 days'
            group by song_id
            having count(song_id) > 23
        ) q
        ), sm_averages as (
        select
            song_id,
            sm_streams_avg,
            sm_streams_rpc,
            (
            case
                when sm_streams_avg <= 1428 then 1
                when sm_streams_avg > 1428 and sm_streams_avg <= 2857 then 2
                when sm_streams_avg > 2857 and sm_streams_avg <= 5714 then 3
                when sm_streams_avg > 5714 and sm_streams_avg <= 14285 then 4
                when sm_streams_avg > 14285 and sm_streams_avg <= 35714 then 5
                when sm_streams_avg > 35714 and sm_streams_avg <= 71428 then 6
                when sm_streams_avg > 71428 and sm_streams_avg <= 142857 then 7
                when sm_streams_avg > 142857 and sm_streams_avg <= 357142 then 8
                when sm_streams_avg > 357142 then 9
            end
            ) as sm_streams_tier
        from (
            select
            song_id,
            round(avg(streams)) as sm_streams_avg,
            round(avg(pct_chg), 2) as sm_streams_rpc
            from streams
            group by song_id
            having count(song_id) > 120
        ) q
        ), results as (
        select
            r.song_id,
            twa.tw_streams_avg,
            twa.tw_streams_rpc,
            twa.tw_streams_tier,
            tma.tm_streams_avg,
            tma.tm_streams_rpc,
            tma.tm_streams_tier,
            sma.sm_streams_avg,
            sma.sm_streams_rpc,
            sma.sm_streams_tier,
            r.signed
        from nielsen_song.reports_recent r
        left join tw_averages twa on r.song_id = twa.song_id
        left join tm_averages tma on r.song_id = tma.song_id
        left join sm_averages sma on r.song_id = sma.song_id
        ), p as (
        select
            avg(tw_streams_rpc) + stddev(tw_streams_rpc) as tw_upper,
            avg(tw_streams_rpc) - stddev(tw_streams_rpc) as tw_lower,
            avg(tw_streams_rpc) as tw_avg,
            avg(tm_streams_rpc) + stddev(tm_streams_rpc) as tm_upper,
            avg(tm_streams_rpc) - stddev(tm_streams_rpc) as tm_lower,
            avg(tm_streams_rpc) as tm_avg,
            avg(sm_streams_rpc) + stddev(sm_streams_rpc) as sm_upper,
            avg(sm_streams_rpc) - stddev(sm_streams_rpc) as sm_lower,
            avg(sm_streams_rpc) as sm_avg
        from results
        ), final as (
        select
            *,
            (
            case
                when tw_streams_rpc < (select tw_lower from p) then 1
                when tw_streams_rpc >= (select tw_lower from p) and tw_streams_rpc < (select tw_avg from p) then 2
                when tw_streams_rpc >= (select tw_avg from p) and tw_streams_rpc < (select tw_upper from p) then 3
                when tw_streams_rpc >= (select tw_upper from p) then 4
                else 5
            end
            ) as tw_streams_status,
            (
            case
                when tm_streams_rpc < (select tm_lower from p) then 1
                when tm_streams_rpc >= (select tm_lower from p) and tm_streams_rpc < (select tm_avg from p) then 2
                when tm_streams_rpc >= (select tm_avg from p) and tm_streams_rpc < (select tm_upper from p) then 3
                when tm_streams_rpc >= (select tm_upper from p) then 4
                else 5
            end
            ) as tm_streams_status,
            (
            case
                when sm_streams_rpc < (select sm_lower from p) then 1
                when sm_streams_rpc >= (select sm_lower from p) and sm_streams_rpc < (select sm_avg from p) then 2
                when sm_streams_rpc >= (select sm_avg from p) and sm_streams_rpc < (select sm_upper from p) then 3
                when sm_streams_rpc >= (select sm_upper from p) then 4
                else 5
            end
            ) as sm_streams_status
        from results
        )

        insert into nielsen_song.daily_streams_cache (
        song_id,
        tw_streams_avg, tw_streams_rpc, tw_streams_tier, tw_streams_status,
        tm_streams_avg, tm_streams_rpc, tm_streams_tier, tm_streams_status,
        sm_streams_avg, sm_streams_rpc, sm_streams_tier, sm_streams_status,
        signed
        )
        select
        song_id,
        tw_streams_avg, tw_streams_rpc, tw_streams_tier, tw_streams_status,
        tm_streams_avg, tm_streams_rpc, tm_streams_tier, tm_streams_status,
        sm_streams_avg, sm_streams_rpc, sm_streams_tier, sm_streams_status,
        signed
        from final;
    """
    db.execute(string)

def cacheProjectReports(settings, db):

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
        from nielsen_song.reports_recent r
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

        with tc as (
        select
            m.id,
            tc.track_count
        from nielsen_artist.meta m
        inner join (
            select unified_artist_id, count(*) as track_count
            from (
            select unified_artist_id, unified_song_id
            from nielsen_map.map
            group by unified_artist_id, unified_song_id
            ) q
            group by unified_artist_id
        ) tc on m.unified_artist_id = tc.unified_artist_id
        )

        update nielsen_artist.meta m
        set track_count = tc.track_count
        from tc
        where m.id = tc.id;

        with tmp as (
        select
            m.id as song_id,
            map.unified_collection_id
        from nielsen_song.meta m
        left join nielsen_map.map on m.unified_song_id = map.unified_song_id
        group by m.id, map.unified_collection_id
        )

        insert into nielsen_song.track_collections (song_id, unified_collection_id)
        select song_id, unified_collection_id from tmp;
    """
    db.execute(string)

class PipelineManager(PipelineBase):

    def __init__(self):
        PipelineBase.__init__(self)

def monday_weekly(settings):

    print('Running monday weekly functions')
    pipe = PipelineManager()

    db = Db('rca_db')
    db.connect()

    cacheChartmetricIds(settings, db)
    pipe.printFnComplete('Cached Chartmetric Ids')

    cacheArtistDiscoveryStats(settings, db)
    pipe.printFnComplete('Cached Artist Discovery Stats')

    cacheStreamingStats(settings, db)
    pipe.printFnComplete('Cached Streaming Stats')

    cacheProjectReports(settings, db)
    pipe.printFnComplete('Cached Project Reports')

    # Only commit our changes if we aren't testing
    if settings['is_testing'] == False:
        db.commit()

    db.disconnect()

    pipe.printSuccess('Completed monday functions')