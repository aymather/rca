from spotipy import util
from .Fuzz import Fuzz
import pandas as pd
import requests
import spotipy
import random


def request_wrapper(func):

    def wrapper(self, *args, **kwargs):

        # Refresh the token
        if random.random() < self.refresh_rate:
            self.refresh()

        count = 0
        while count < self.retries:

            try:

                # Search spotify api
                res = func(self, *args, **kwargs)

                return res

            except Exception as e:
                print(str(e))

            count += 1

        raise Exception(f'Error getting spotify data with query: ' + str(args[0]))

    return wrapper

# Class for making spotify client handling easier
class Spotify:
    
    def __init__(self):
        
        # Spotify Authentication
        self.client_id='13b0d9dd85864779a5af921822875398'
        self.client_secret='ca3119ddfaa24699a3b80d9c969329c7'
        self.redirect_uri='https://google.com'
        self.auth_token = None
        self.username = 'Aaron Dombey'
        self.scope = 'playlist-modify-public'
        self.retries = 5
        self.refresh_rate = 0.1 # Every time we search for something we're gunna randomly refresh the access token at this rate

        self.sp = self.create()

    def test(self):

        """
            Simple method to test and make sure that spotify's api is available and working.
        """

        self.getArtistByName('John Mayer')
        print('Successful connection to Spotify Api')
        
    def refresh(self):
        self.sp = self.create()
        
    def create(self):
        
        spotify_token = util.prompt_for_user_token(self.username,
                                                   self.scope,
                                                   client_id=self.client_id,
                                                   client_secret=self.client_secret,
                                                   redirect_uri=self.redirect_uri)

        if spotify_token is None:
            raise Exception('Error getting spotify token')

        self.auth_token = 'Bearer ' + spotify_token

        sp = spotipy.Spotify(
            auth=spotify_token,
            requests_timeout=10,
            retries=10,
            status_retries=10
        )
        
        return sp
    
    def get(self, url):
        
        headers = { 'Authorization': self.auth_token }
    
        res = requests.get(url, headers=headers) # type: ignore

        if res.status_code < 400:

            return res.json()

        return None

    @request_wrapper
    def searchArtists(self, q, limit=25):

        """
            Generic search function to make requests to spotify for artists.
        """

        # Search spotify api
        res = self.sp.search(q=q, type='artist', limit=limit)

        if res is None:
            raise Exception('Error getting spotify data from query: ' + q)

        items = [i for i in res['artists']['items'] if i is not None]
        return items

    @request_wrapper
    def searchTracks(self, q, limit=25):

        """
            Generic search function to make requets to spotify for tracks.
        """

        # Search spotify api
        res = self.sp.search(q=q, type='track', limit=limit)

        if res is None:
            raise Exception('Error getting spotify data from query: ' + q)

        items = [i for i in res['tracks']['items'] if i is not None]
        return items
    
    @request_wrapper
    def albums(self, album_ids, parse=False):
        """Maximum of 20 ids"""
        
        res = self.sp.albums(album_ids)

        if res is None:
            raise Exception('Error getting albums by spotify album ids: ' + ','.join(album_ids))

        res = res['albums']
        res = [i for i in res if i is not None]

        if parse is True:
            return [transformAlbumData(i) for i in res]

        return res

    @request_wrapper
    def tracks(self, track_ids, parse=False):
        """Maximum of 50 ids"""

        res = self.sp.tracks(track_ids)

        if res is None:
            raise Exception('Error getting tracks by spotify track ids: ' + ','.join(track_ids))

        res = res['tracks']
        res = [i for i in res if i is not None]

        if parse is True:
            return [transformTrackData(i) for i in res]

        return res

    @request_wrapper
    def artists(self, artist_ids, parse=False):
        """Maximum of 50 ids"""

        res = self.sp.artists(artist_ids)

        if res is None:
            raise Exception('Error getting artists by spotify artist ids: ' + ','.join(artist_ids))

        res = res['artists']
        res = [i for i in res if i is not None]

        # If we don't want the raw data, transform it
        if parse is True:
            res = [transformArtistData(i) for i in res]

        return res

    @request_wrapper
    def audio_features(self, track_ids):
        """Maximum of 100 ids"""

        res = self.sp.audio_features(track_ids)

        if res is None:
            raise Exception('Error getting audio features with spotify track ids: ' + ','.join(track_ids))

        res = [i for i in res if i is not None]
        return res

    @request_wrapper
    def artist_top_tracks(self, artist_id, parse=False):

        res = self.sp.artist_top_tracks(artist_id)

        if res is None:
            raise Exception('Error getting artist top tracks with spotify artist ids: ' + ','.join(artist_id))

        res = res['tracks']
        res = [i for i in res if i is not None]

        if parse is True:
            return [transformArtistTopTrack(i) for i in res]

        return res

    def artist_top_track(self, artist_id, parse=False):
        """Get the top track for an artist"""

        res = self.artist_top_tracks(artist_id)

        if len(res) == 0:
            return None

        if parse is True:
            return transformArtistTopTrack(artist_id, res[0])
        
        return res[0]

    def getArtistByName(self, name):

        """
            This function does a little bit of work to try to find an individual artist,
            returns None if it is unable to find a good match.
        """

        name = name.replace('[', '').replace(',', '').replace(']', '').replace('&', ' ').split('Feat.')[0][:100]

        if len(name) == 0:
            return None
        
        # Query the spotify api
        items = self.searchArtists(name, limit=20)
        
        # Check for a perfect match on first item, if we find one, just return that
        if len(items) > 0 and items[0]['name'] == name:
            result = items[0]
            return pd.DataFrame({ 'name': [result['name']], 'spotify_id': [result['id']] })
        
        # Get the names from the results to prepare for fuzzy matching
        artist_names = [i['name'] for i in items]
        
        # Fuzzy match
        fuzz = Fuzz(artist_names)
        ratio, match, _ = fuzz.check(name)
        
        # Threshold is 0.9 for correctness
        if ratio >= 0.9:
            idx = artist_names.index(match)
            result = items[idx]
            return pd.DataFrame({ 'name': [result['name']], 'spotify_id': [result['id']] })
            
        # Else return None
        return None
    
    # This is essentially the same as self.getArtistByName but returns the entire artist object
    def searchArtistByName(self, name):

        name = name.replace('[', '').replace(',', '').replace(']', '').replace('&', ' ').split('Feat.')[0][:100]

        if len(name) == 0:
            return None

        # Query the spotify api
        items = self.searchArtists(name, limit=20)
        
        # Check for a perfect match on first item, if we find one, just return that
        if len(items) > 0 and items[0]['name'] == name:
            result = items[0]
            return result
        
        # Get the names from the results to prepare for fuzzy matching
        artist_names = [i['name'] for i in items]
        
        # Fuzzy match
        fuzz = Fuzz(artist_names)
        ratio, match, _ = fuzz.check(name)
        
        # Threshold is 0.9 for correctness
        if ratio >= 0.9 and match is not None:
            idx = artist_names.index(match)
            result = items[idx]
            return result
            
        # Else return None
        return None
    
    def searchByTitleAndArtist(self, track, artist):
        
        # Extract artist & track (and clean artist name)
        artist = ' '.join(str(artist).replace('[', '').replace(']', '').replace('&', ' ').replace('Feat.', ' ').replace(',', ' ').replace('(', ' ').replace(')', ' ').split(' ~ ')[0].split(' - ')[0].split())[:37]
        title = ' '.join(str(track).replace('[', '').replace(']', '').replace('&', ' ').replace(',', ' ').replace('(', ' ').replace(')', ' ').split('Feat.')[0].strip().split(' - ')[0].split())[:37]

        if len(artist) == 0 or len(title) == 0:
            return None

        # Build query
        q = 'track:{} artist:{}'.format(title, artist)

        track_items = self.searchTracks(q, limit=10)

        """
        STRATEGY 1:
        Create a list of all the track names, and fuzzy match our track
        name to all the returned tracks to see if any of them are a good
        match.
        Also alphabetize everything to prevent unordered consequences.
        """

        # If we got results, proceed with this strategy
        if len(track_items) > 0:

            # Create fuzzy set for checking results against our track name
            tracknames = []
            artists = []
            for i in range(len(track_items)):

                tracknames.append(track_items[i]['name'])

                sub_artists = []
                if track_items[i]['artists'] is not None and len(track_items[i]['artists']) > 0:
                    for a in track_items[i]['artists']:
                        sub_artists.append(a['name'])
                    artists.append(' '.join(sub_artists))

            # Create trackname Fuzz
            tracks_fuzz = Fuzz(tracknames)

            # Check our track name against Fuzz
            track_ratio, track_match, _ = tracks_fuzz.check(track)

            if track_ratio >= 0.75 and track_match is not None:

                idx = tracknames.index(track_match)
                info = track_items[idx]
                return info

            """
            STRATEGY 2:
            We can do substring matching with the original results,
            but this can be sketchy, so in order for this to pass it
            must have a solid substring match in both the track name
            and artist name.
            """

            # Create a Fuzz for artists as well
            artists_fuzz = Fuzz(artists)

            # Loop through artist/tracknames
            for i in range(len(artists)):

                a = artists[i]
                t = tracknames[i]

                artist_ratio, _, _ = artists_fuzz.check(a)
                track_ratio, _, _ = tracks_fuzz.check(t)

                if artist_ratio >= 0.8 and track_ratio >= 0.8:
                    info = track_items[i]
                    return info
        """
        STRATEGY 3:
        Search for just the track name in the spotify api,
        aggregate all the artist names of all the tracks returned,
        then fuzzy match our artist to any of the artists returned in
        the list of tracks.
        Again, alphabetize the resulting artist names to prevent unordered consequences.
        """

        track_q = 'track:{}'.format(title)

        # Try searching by just the track name
        track_items = self.searchTracks(track_q, limit=20)

        if len(track_items) > 0:

            # Transform the list of tracks' artists into an array of artists
            artists = []
            for item in track_items:

                sub_artists = []
                if item['artists'] is not None and len(item['artists']) > 0:
                    for a in item['artists']:
                        sub_artists.append(a['name'])
                    artists.append(' '.join(sub_artists))

            # Create Fuzz for artists
            artists_fuzz = Fuzz(artists)

            # Check our artist against the array
            fuzz_ratio, fuzz_match, _ = artists_fuzz.check(artist)

            # A solid score for an artist name and we can assume we found it
            if fuzz_ratio >= 0.75:
                idx = artists.index(fuzz_match)
                return track_items[idx]

        # If we still haven't found anything, return None
        return None
    
"""

    Functions for transforming the data from the Spotify API

"""

def getSpotifyImage(images, size='large'):

    if len(images) == 0:
        return None
    
    if size == 'large':
        images.sort(key=lambda x: x['height'], reverse=True)
        return images[0]['url']
    elif size == 'small':
        images.sort(key=lambda x: x['height'])
        return images[0]['url']
    else:
        return None

def transformArtistData(artist):
    """
        Transform the artist data into a dictionary
    """

    spotify_artist_id = artist['id']
    url = artist['external_urls']['spotify'] if 'external_urls' in artist and 'spotify' in artist['external_urls'] else None
    followers = artist['followers']['total'] if 'followers' in artist and 'total' in artist['followers'] else None
    genres = '/'.join(artist['genres']) if len(artist['genres']) > 0 else None
    api_url = artist['href'] if 'href' in artist else None
    name = artist['name'] if 'name' in artist else None
    popularity = artist['popularity'] if 'popularity' in artist else None
    uri = artist['uri'] if 'uri' in artist else None
    name = artist['name'] if 'name' in artist else None
    spotify_image = getSpotifyImage(artist['images'], size='large')

    return {
        'spotify_artist_id': spotify_artist_id,
        'followers': followers,
        'url': url,
        'genres': genres,
        'api_url': api_url,
        'name': name,
        'popularity': popularity,
        'uri': uri,
        'spotify_image': spotify_image,
        'name': name
    }

def transformArtistTopTrack(spotify_artist_id, track):

    spotify_popular_track_id = track['id']
    spotify_popular_album_id = track['album']['id'] if 'album' in track and 'id' in track['album'] else None

    return {
        'spotify_artist_id': spotify_artist_id,
        'spotify_popular_track_id': spotify_popular_track_id,
        'spotify_popular_album_id': spotify_popular_album_id
    }

def transformTrackData(track):

    disc_number = track['disc_number']
    duration_ms = track['duration_ms']
    explicit = track['explicit']
    isrc = track['external_ids']['isrc'] if 'external_ids' in track and 'isrc' in track['external_ids'] else None
    popularity = track['popularity']
    spotify_track_id = track['id']
    track_number = track['track_number']
    name = track['name']
    preview_url = track['preview_url']
    spotify_album_id = track['album']['id'] if 'album' in track and 'id' in track['album'] else None
    spotify_artist_id = track['artists'][0]['id'] if 'artists' in track and len(track['artists']) > 0 and 'id' in track['artists'][0] else None
    spotify_image = getSpotifyImage(track['album']['images']) if 'album' in track and 'images' in track['album'] else None

    return {
        'disc_number': disc_number,
        'duration_ms': duration_ms,
        'explicit': explicit,
        'isrc': isrc,
        'popularity': popularity,
        'spotify_track_id': spotify_track_id,
        'track_number': track_number,
        'name': name,
        'preview_url': preview_url,
        'spotify_album_id': spotify_album_id,
        'spotify_artist_id': spotify_artist_id,
        'spotify_image': spotify_image
    }

def getCopyrights(copyrights):

    if len(copyrights) == 0:
        return None

    return '/'.join([copyright['text'] for copyright in copyrights])

def transformAlbumData(album):

    spotify_album_id = album['id']
    spotify_copyrights = getCopyrights(album['copyrights'])
    spotify_label = album['label'] if 'label' in album else None

    return {
        'spotify_album_id': spotify_album_id,
        'spotify_copyrights': spotify_copyrights,
        'spotify_label': spotify_label
    }