from .Db import Db
from .Spotify import Spotify
from .Time import Time
from .functions import chunker, print_memory_usage
import pandas as pd

def recacheSpotifyArtists():

    time = Time()

    # Connect clients
    db = Db('rca_db_prod')
    db.connect()

    sp = Spotify()

    # Get list of artists to recache
    string = """
        select distinct spotify_artist_id
        from (
            select spotify_artist_id
            from nielsen_artist.spotify
            where spotify_artist_id is not null
            order by last_updated
        ) q
        limit 2000
    """
    df = db.execute(string)
    print(f'Found {len(df)} artists to recache.')

    # Extract the spotify artist ids
    spotify_artist_ids = df.spotify_artist_id.tolist()
    chunks = chunker(spotify_artist_ids, 50) # split into chunks of 50

    # Get the artist data from Spotify API
    data = []
    for idx, chunk in enumerate(chunks):
        print(f'\rGetting artist chunk {idx+1} of {len(chunks)}...', end='', flush=True)

        artists = sp.artists(chunk, parse=True)
        data = [ *data, *artists ]

    print('\n')

    # Convert to dataframe
    data = pd.DataFrame(data)

    # Merge the data back onto the original dataframe
    df = pd.merge(df, data, on='spotify_artist_id', how='left')

    # Fill na values for popularity & followers
    df['popularity'] = df['popularity'].fillna(0)
    df['followers'] = df['followers'].fillna(0)

    # Clean types
    df['popularity'] = df['popularity'].astype(int)
    df['followers'] = df['followers'].astype(int)

    # Now we have to use the same spotify artist ids to get the top tracks for each artist
    top_tracks = []
    for idx, spotify_artist_id in enumerate(spotify_artist_ids):
        print(f'\rGetting top track for artist {idx+1} of {len(spotify_artist_ids)}...', end='', flush=True)

        track = sp.artist_top_track(spotify_artist_id, parse=True)

        if track is not None:
            top_tracks.append(track)

    print('\n')

    # Convert to dataframe
    top_tracks = pd.DataFrame(top_tracks)

    # Merge the data back onto the original dataframe
    df = pd.merge(df, top_tracks, on='spotify_artist_id', how='left')

    # Extract the top track ids excluding null values
    top_album_ids = df['spotify_popular_album_id'].dropna().tolist()

    # Split into chunks of 20
    chunks = chunker(top_album_ids, 20)
    data = []
    for idx, chunk in enumerate(chunks):
        print(f'\rGetting album chunk {idx+1} of {len(chunks)}...', end='', flush=True)

        albums = sp.albums(chunk, parse=True)
        data = [ *data, *albums ]

    print('\n')

    data = pd.DataFrame(data)

    # Merge the data back onto the original dataframe
    df = pd.merge(df, data, left_on='spotify_popular_album_id', right_on='spotify_album_id', how='left')
    df.drop(columns=['spotify_album_id'], inplace=True) # we don't need this column anymore

    print('Finished getting spotify data.')

    # Create a temp table containing our new data
    string = """
        create temp table data (
            artist_id int,
            spotify_artist_id text,
            followers int,
            genres text,
            popularity int,
            last_updated timestamp,
            url text,
            api_url text,
            name text,
            spotify_image text,
            uri text,
            spotify_popular_track_id text,
            spotify_popular_album_id text,
            spotify_copyrights text,
            spotify_label text
        )
    """
    db.execute(string)
    db.big_insert(df, 'data')

    # Upsert the data into the spotify table
    string = """
        update nielsen_artist.spotify sp
        set
            followers = data.followers,
            genres = data.genres,
            popularity = data.popularity,
            last_updated = current_timestamp,
            url = data.url,
            api_url = data.api_url,
            name = data.name,
            spotify_image = data.spotify_image,
            uri = data.uri,
            spotify_popular_track_id = data.spotify_popular_track_id,
            spotify_popular_album_id = data.spotify_popular_album_id,
            spotify_copyrights = data.spotify_copyrights,
            spotify_label = data.spotify_label
        from data
        where sp.spotify_artist_id = data.spotify_artist_id
    """
    db.execute(string)

    string = """drop table data"""
    db.execute(string)

    print('Done.')
    print_memory_usage()
    time.printElapsed()