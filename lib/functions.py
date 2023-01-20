from IPython.display import clear_output
from datetime import datetime as dt
from .Spotify import Spotify
import pandas as pd
import random
import string
import shutil


def today():
    return dt.strftime(dt.today(), '%Y-%m-%d')

def clear():
    clear_output(wait=True)

def createId(str_size = 7):
    return ''.join(random.choice(string.ascii_uppercase) for x in range(str_size))

# Loop over an array in chunks
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

# Create a zip file
def make_archive(folder_name, archive_fullfile):

    """
        params:
            @folder_name | The path to the folder that you want to archive
            @archive_fullfile | The full path to where you want the archive to go
    """

    shutil.make_archive(archive_fullfile, 'zip', folder_name)

def getSpotifyTrackDataFromSpotifyUsingIsrcTitleAndArtist(df):

    """
    
        @param df('isrc', 'artist', 'title')

        Function is separated into 3 sections
            1. Get track level data
            2. Get album level data
            3. Get audio feature data

        NOTE: The isrc returned from this function will default to the one passed in the 'isrc' column, however, if that value
              is None then it will return the isrc that we got from the spotify api. The reason we can do this is because if
              the isrc passed is None, we use the artist/title values to search spotify instead.

        @returns df('isrc', 'artist', 'title', **spotify_track_columns)
    
    """

    # Make sure that we have the required columns in our input df
    if 'isrc' not in df.columns or 'artist' not in df.columns or 'title' not in df.columns:
        raise Exception(f'Missing required column: isrc, artist, title in {list(df.columns)}')

    # Return early if the dataframe is empty
    if df.empty:
        return df

    spotify = Spotify()

    def getSpotifyTrackData(df):

        def attachTrackDataByRow(row):

            def getByTitleAndArtist(isrc, title, artist):

                res = spotify.searchByTitleAndArtist(title, artist)

                if res is not None:
                    return extractSongInfo(isrc, res)

                return pd.Series(tuple([None for i in range(len(cols))]))

            def extractSongInfo(isrc, song):

                def get_image(arr):

                    if len(arr) == 0:
                        return ''
                    else:
                        arr.sort(key=lambda x: x['height'], reverse=True)
                        return arr[0]['url']

                def get_isrc(isrc, song):

                    if isrc is not None:
                        return isrc

                    if 'external_ids' in song and 'isrc' in song['external_ids']:
                        return song['external_ids']['isrc']

                    return None

                disc_number = song['disc_number']
                duration_ms = song['duration_ms']
                explicit = song['explicit']
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

                isrc = get_isrc(isrc, song)

                return pd.Series((isrc, disc_number, duration_ms, explicit, url,
                                    api_url, spotify_track_id, spotify_artist_id, spotify_album_id, is_local,
                                    name, popularity, preview_url, track_number, uri, album_type, spotify_image,
                                    release_date, total_tracks))

            isrc = row['isrc']

            if pd.notnull(isrc):

                items = spotify.searchTracks(f'isrc:{isrc}')
                
                if len(items) > 0:
                    return extractSongInfo(isrc, items[0])

            return getByTitleAndArtist(isrc, row['title'], row['artist'])

        cols = [
            'isrc',
            'disc_number',
            'duration_ms',
            'explicit',
            'url',
            'api_url',
            'spotify_track_id',
            'spotify_artist_id',
            'spotify_album_id',
            'is_local',
            'name',
            'popularity',
            'preview_url',
            'track_number',
            'uri',
            'album_type',
            'spotify_image',
            'release_date',
            'total_tracks'
        ]

        df[cols] = df.apply(attachTrackDataByRow, axis=1)

        return df

    def getSpotifyAlbumData(df):

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
        album_ids = df.loc[(~df['spotify_album_id'].isnull()) & (df['spotify_album_id'] != ''), 'spotify_album_id'].unique()

        # Loop through in chunks of 20
        data = []
        chunks = chunker(album_ids, 20)
        for i, chunk in enumerate(chunks):

            items = [album2Data(i) for i in spotify.albums(chunk)]

            data = [ *data, *items ]

        # Convert this to a dataframe
        data = pd.DataFrame(data)

        # If we have nothing to return, then just return the dataframe with the columns we need as null
        if data.shape[0] == 0:

            df[['copyrights', 'label', 'upc', 'genre', 'album_name']] = None

        else:

            # Merge the copyright information onto the dataframe
            df = pd.merge(df, data, on='spotify_album_id', how='left')

        return df

    def getSpotifyAudioFeatureData(df):

        # Get all the spotify track ids
        track_ids = df.loc[(~df['spotify_track_id'].isnull()) & (df['spotify_track_id'] != ''), 'spotify_track_id'].unique()

        # Loop through in chunks of 100
        data = []
        max_chunk = 100
        chunks = chunker(track_ids, max_chunk)
        for i, chunk in enumerate(chunks):

            items = spotify.audio_features(chunk)
            data = [ *data, *items ]

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

        # Clean the types on int columns
        int_cols = ['disc_number', 'duration_ms', 'popularity', 'track_number', 'total_tracks', 'key', 'mode', 'time_signature']
        df[int_cols] = df[int_cols].fillna(0)
        df[int_cols] = df[int_cols].astype(int)

        # Sometimes dates will only have the year so we just need to add some formatting there
        mask = df['release_date'].str.len() == 4
        df.loc[mask, 'release_date'] = df[mask].apply(lambda x: x.release_date + '-01-01', axis=1)

        mask = df['release_date'].str.len() == 7
        df.loc[mask, 'release_date'] = df[mask].apply(lambda x: x.release_date + '-01', axis=1)
        
        return df
    
    df = getSpotifyTrackData(df)
    df = getSpotifyAlbumData(df)
    df = getSpotifyAudioFeatureData(df)

    return df