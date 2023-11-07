from tenacity import retry, stop_after_attempt, wait_fixed
from IPython.display import clear_output
import Levenshtein as lev
from datetime import datetime as dt
from .Spotify import Spotify
from urllib.request import urlopen
from colorthief import ColorThief
from io import BytesIO
import pandas as pd
import psutil
import random
import string
import shutil
import os


def request_wrapper(func):

    """

        Wrapper on top of requests for retries
    
    """
    
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.5))
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper

def test():
    print('RCA Library Test Function: ' + dt.now().strftime('%Y-%m-%d %H:%M:%S'))

def getDominantColor(image_url):

    try:

        # Load the remote image into memory
        with urlopen(image_url) as response:
            image_data = response.read()

        # Create a BytesIO object from the image data
        image_buffer = BytesIO(image_data)

        color_thief = ColorThief(image_buffer)
        color = color_thief.get_color(quality=1)

        return rgb2Hex(color)
    
    except:
        # If we get any error while trying to load the new color, return default color
        # with a slight modification so that we know that we at least tried to get a new color
        return '#000001'
    
def rgb2Hex(rgb):

    # Convert the RGB tuple to a hex color string
    hex_color = '#{:02x}{:02x}{:02x}'.format(*rgb)
    return hex_color

def today():
    return dt.strftime(dt.today(), '%Y-%m-%d')

def clear():
    clear_output(wait=True)

def createId(str_size = 7):
    return ''.join(random.choice(string.ascii_uppercase) for x in range(str_size))

# Loop over an array in chunks
def chunker(seq, size):
    return list(seq[pos:pos + size] for pos in range(0, len(seq), size))

# Create a zip file
def make_archive(folder_name, archive_fullfile):

    """
        params:
            @folder_name | The path to the folder that you want to archive
            @archive_fullfile | The full path to where you want the archive to go
    """

    shutil.make_archive(archive_fullfile, 'zip', folder_name)

def get_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024 * 1024)

def print_memory_usage():
    current_memory_usage = get_memory_usage()
    print(f"Current memory usage: {current_memory_usage:.2f} MB")

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

def match2Nielsen(df, db):

    # Shazam match 2 nielsen
    def hasMatch(a, t, n_artists, n_titles):
        
        for i in range(len(n_artists)):
            
            n_artist = n_artists[i].lower()
            n_title = n_titles[i].lower()
            
            if (lev.ratio(a, n_artist) > 0.75) or \
            (n_artist.find(a) != -1) or \
            (a.find(n_artist) != -1):
                
                if lev.ratio(t, n_title) > 0.75:
                    
                    return i
                
                else:
                    
                    n_title = n_title.split('(')[0]
                    n_title = str(n_title)
                    n_title = n_title.split('   ')[0]
                    n_title = str(n_title)

                    if lev.ratio(t, n_title) > 0.75:
                        return i
                
        return False

    def match2NielsenFilter(row, n_artists, n_titles, n_song_id):
        
        a = row['artist'].lower()
        t = row['title'].lower()
        
        idx = hasMatch(a, t, n_artists, n_titles)
        if idx != False:
        
            return n_song_id[idx]
        
        return None
    
    string = """
        select
            rr.song_id,
            m.artist,
            m.title
        from nielsen_song.__reports_recent rr
        join nielsen_song.meta m on rr.song_id = m.id
    """
    nielsen = db.execute(string)
    
    # Convert to numpy arrays
    n_artists = nielsen['artist'].values
    n_titles = nielsen['title'].values
    n_song_id = nielsen['song_id'].values
    
    # Run function
    df['song_id'] = df.apply(match2NielsenFilter, n_artists=n_artists, n_titles=n_titles, n_song_id=n_song_id, axis=1)
    
    return df

def is_array_in_string(substring_array, string):
    """Check if any of the substrings in an array are in a string.
    
    This function is basically the same idea as `is_substring_in_array`, except
    it's the reverse. It checks if any of the strings in the array are contained
    in the single string.
    
    Parameters
    ----------
    substring_array : str[]
        The array of substrings to check for.
    string : str
        The string to check in.
    
    Returns
    -------
    bool
        True if any of the substrings in the array are contained in the string, False otherwise.
    
    Examples
    --------
    >>> from rca.lib.etc import is_array_in_string
    >>> is_array_in_string(['abc', 'def'], 'abcdef')
    True
    >>> is_array_in_string(['abc', 'def'], 'ghi')
    False
    """
    
    for substring in substring_array:
        if substring in string:
            return True
    return False

def filter_signed_artists_with_nielsen_label_list(df, db):
    """Filter a dataframe of artists based on the nielsen labels list in the database: `misc.nielsen_labels`
    
    This function takes in a dataframe with a `label` column which is the label name
    given to us in the nielsen data. They have a very specific process for naming labels
    that are associated with each song, and we can use this to determine if the artist
    is signed or not.

    The `df` passed is expected to have a column called `label` which is the label name from nielsen.
    The `df` can also optionally have an existing `signed` column which is a boolean value
        representing whether the artist is signed or not. If this column does not exist, it will
        be created.
    
    Parameters
    ----------
    df[label, signed]
        The dataframe to filter. Must have a `label` column.
    db : Db
        Database connection: rca_db_prod
    
    Returns
    -------
    df[label, signed]
        The same dataframe that was passed, but with a `signed` column altered / added.
    
    Examples
    --------
    >>> from rca.lib.processing.nielsen.us_daily.utils.signed_filters import filter_signed_artists_with_nielsen_label_list
    >>> from rca.lib.clients import Db
    >>> db = Db('rca_db_prod')
    >>> df = filter_signed_artists_with_nielsen_label_list(df, db)
    >>> df.head()
        label  signed
    0   label1    True
    1   label2   False
    2   label3    True
    """

    # Validate that the df has a label column
    if 'label' not in df.columns:
        raise ValueError('The dataframe passed to `filter_signed_artists_with_nielsen_label_list` must have a `label` column.')
    
    # Check if the `signed` column exists
    # If not, create it and assume that none of the artists are signed
    if 'signed' not in df.columns:
        df['signed'] = False

    # Get the list of nielsen labels from the database.
    # This list has been specifically curated for this process.
    # For example, in this list we have `Columbia` because we know that if a record
    # is released on `Columbia`, then it is a signed record.
    # However, this list does not include certain nielsen labels such as `The Orchard`
    # because we still consider that a competivie record that we could potentially go after.
    nielsen_labels_df = db.execute('select lower(label) as label from misc.nielsen_labels')
    nielsen_labels = nielsen_labels_df.label.values

    def fn(row):

        # If the `signed` column is already True, return True
        if row['signed'] is True:
            return True

        # If the label is null, return False
        if pd.isnull(row['label']):
            return False
        
        # If the label is in the nielsen labels list, return True
        if is_array_in_string(nielsen_labels, row['label'].lower()):
            return True
        
        # Otherwise, just assume that the artist is not signed
        return False

    df['signed'] = df.apply(fn, axis=1)

    return df

