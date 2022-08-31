from spotipy import util
from .Fuzz import Fuzz
import pandas as pd
import requests
import spotipy

# We will retry on any status code higher than 400
status_forcelist = tuple( x for x in requests.status_codes._codes if x >= 400 )

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

        self.sp = self.create()

    def test(self):

        """
            Simple method to test and make sure that spotify's api is available and working.
        """

        try:

            self.getArtistByName('John Mayer')
            print('Successful connection to Spotify Api')

        except Exception as e:
            raise(f'Error connecting to Spotify Api: {str(e)}')
        
    def refresh(self):
        
        self.sp = self.create()
        
    def create(self):
        
        spotify_token = util.prompt_for_user_token(self.username,
                                                   self.scope,
                                                   client_id=self.client_id,
                                                   client_secret=self.client_secret,
                                                   redirect_uri=self.redirect_uri)
        self.auth_token = 'Bearer ' + spotify_token

        sp = spotipy.Spotify(
            auth=spotify_token,
            requests_timeout=10,
            retries=3,
            status_forcelist=status_forcelist,
            status_retries=3
        )
        
        return sp
    
    def get(self, url):
        
        headers = { 'Authorization': self.auth_token }
    
        res = requests.get(url, headers=headers)

        if res.status_code < 400:

            return res.json()

        return None
    
    def getArtistByName(self, name):

        name = name.replace('[', '').replace(',', '').replace(']', '').replace('&', ' ').split('Feat.')[0][:100]

        if len(name) == 0:
            return None
        
        # Query the spotify api
        results = self.sp.search(name, limit=20, type='artist')
        
        # Filter results to exclude NoneTypes
        items = [i for i in results['artists']['items'] if i is not None]
        
        # Check for a perfect match on first item, if we find one, just return that
        if len(items) > 0 and items[0]['name'] == name:
            result = items[0]
            return pd.DataFrame({ 'name': [result['name']], 'spotify_id': [result['id']] })
        
        # Get the names from the results to prepare for fuzzy matching
        artist_names = [i['name'] for i in items]
        
        # Fuzzy match
        fuzz = Fuzz(artist_names)
        ratio, match, isExact = fuzz.check(name)
        
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
        results = self.sp.search(name, limit=20, type='artist')
        
        # Filter results to exclude NoneTypes
        items = [i for i in results['artists']['items'] if i is not None]
        
        # Check for a perfect match on first item, if we find one, just return that
        if len(items) > 0 and items[0]['name'] == name:
            result = items[0]
            return result
        
        # Get the names from the results to prepare for fuzzy matching
        artist_names = [i['name'] for i in items]
        
        # Fuzzy match
        fuzz = Fuzz(artist_names)
        ratio, match, isExact = fuzz.check(name)
        
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

        # Hit the spotify api
        results = self.sp.search(q=q, type="track", limit = 10)

        """
        STRATEGY 1:
        Create a list of all the track names, and fuzzy match our track
        name to all the returned tracks to see if any of them are a good
        match.
        Also alphabetize everything to prevent unordered consequences.
        """

        # Extract results and make sure we at least got something
        # If track is not available in our market, then it gets returned as None
        # so we need to filter that out.
        track_items = [i for i in results['tracks']['items'] if i is not None]

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
        track_results = self.sp.search(q=track_q, type='track', limit=20)

        # Check if we got any results
        track_items = [i for i in track_results['tracks']['items'] if i is not None]
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