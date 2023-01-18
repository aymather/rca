from IPython.display import clear_output
from datetime import datetime as dt
from .Db import Db
from .Fuzz import Fuzz
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

def findSignedByCopyrights(df):

    """

        Basic check between copyrights and our list of labels.

        Uses: list_of_labels.csv

        @param df(copyrights, signed, *)
        @returns df(copyrights, signed, *)

    """
    
    def findSignedByCopyrightsFilter(row, labels):
        
        if row['signed'] == True:
            return True

        # Extract values
        df_label = row['copyrights'].lower()

        # Find matches
        res = [ele for ele in labels if(ele.lower() in df_label)]
        res = bool(res)

        if res == True:
            return True
        else:
            return False

    # Load in nielsen_labels
    db = Db('rca_db_prod')
    db.connect()

    string = """ select * from misc.nielsen_labels """
    nielsen_labels = db.execute(string)

    db.disconnect()

    labels = nielsen_labels['label'].values

    # Fill na values to avoid errors
    df['copyrights'] = df['copyrights'].fillna('')

    # Apply filter fn
    df['signed'] = df.apply(findSignedByCopyrightsFilter, labels=labels, axis=1)

    return df

def basicSignedCheck(df):

    """
        @param | df with columns: copyrights(str) | signed(bool)
    """

    # Apply another layer of detecting 'signed' with a running list of signed artists
    def filterBySignedArtistsList(df):
        
        def filterBySignedArtistsListFilter(row, fuzz):
    
            # If we already know they're signed, return
            if row['signed'] == True:
                return True

            # Preprocess the artist name
            artist = row['artist']

            # Check against fuzzyset
            ratio, _, _ = fuzz.check(artist)

            if ratio >= 0.95:
                return True
            else:
                return False

        # Get the table of signed artists from db
        db = Db('rca_db_prod')
        db.connect()

        string = """ select * from misc.signed_artists """
        artists_df = db.execute(string)

        db.disconnect()

        artists = artists_df.drop_duplicates(keep='first').artist.values

        # Create fuzzyset
        fuzz = Fuzz(artists)

        # Check each artist name against fuzzyset and determine if they are signed
        df['signed'] = df.apply(filterBySignedArtistsListFilter, fuzz=fuzz, axis=1)

        return df
    
    df = findSignedByCopyrights(df)
    
    if 'artist' in df:
        df = filterBySignedArtistsList(df)
    
    return df