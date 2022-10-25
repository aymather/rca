from rca import Db, Sftp, MAPPING_TABLE_FOLDER
from .PipelineBase import PipelineBase
from .Time import Time
from datetime import datetime as dt
import pandas as pd
import os

def download_files(pipe):

    """
        Download mapping table files.
    """

    # Create client for mapping table server
    sftp = Sftp('mapping_table')

    with sftp.connect() as conn:

        # Download each file
        for idx, (local_fullfile, remote_fullfile) in enumerate(zip(pipe.files['LOCAL_FILES'], pipe.files['REMOTE_FILES'])):

            # We're only going to download it if it doesn't already exist, the end of this script
            # removes all of these files so until then, they're in a "cached" state, which will be helpful
            # if we have to debug an error, but we don't want to have to redownload everything every time
            # we retry the script.
            if os.path.exists(local_fullfile):
                print(f'Local file {local_fullfile} already exists...skipping...')
                continue

            print(f'Downloading {idx + 1}/{pipe.TOTAL_FILES_COUNT}')
            conn.get(remote_fullfile, local_fullfile)

def clean_mapping_file(local_fullfile, cleaned_fullfile):

    """
        Cleans a mapping file of backslashes.

        @param local_fullfile | Path to file that we want to clean
        @param cleaned_fullfile | Path to the cleaned version of the same file
    """
    
    with open(local_fullfile, 'r') as f, open(cleaned_fullfile, 'w') as fo:
        for line in f:
            fo.write(line.replace('\\', '').replace('"', ''))

def clean_files(pipe):

    """
        Clean the local mapping table files.
    """

    # Clean each file
    for idx, (local_fullfile, local_cleaned_fullfile) in enumerate(zip(pipe.files['LOCAL_FILES'], pipe.files['LOCAL_CLEANED_FILES'])):

        # Only clean it if it hasn't already been cleaned, same as before, we'll delete it at the end
        # so until then it'll be in a "cached" state.
        if os.path.exists(local_cleaned_fullfile):
            print(f'File {local_cleaned_fullfile} already cleaned...skipping...')
            continue

        print(f'Cleaning {idx + 1}/{pipe.TOTAL_FILES_COUNT}')
        clean_mapping_file(local_fullfile, local_cleaned_fullfile)

def read_file(local_cleaned_fullfile):

    """
        All our files need to read the same way, and we need a few parameters
        for that. Pass the local cleaned file.
    """
    return pd.read_csv(local_cleaned_fullfile, delimiter='\t', on_bad_lines='skip')

def create_map(map_files):

    # Read and concatenate all the files
    df = pd.concat([ read_file(fullfile) for fullfile in map_files ])

    # Drop duplicates and reset index
    df = df.drop_duplicates(subset=['UnifiedSongID', 'Unified Artist ID', 'Unified Collection ID']).reset_index(drop=True)

    # Rename columns
    cols_rename = {
        'UnifiedSongID': 'unified_song_id',
        'Unified Artist ID': 'unified_artist_id',
        'Unified Collection ID': 'unified_collection_id'
    }
    df.rename(columns=cols_rename, inplace=True)
    
    # First convert to Int64 to remove .0's from everything
    types = { 'unified_artist_id': 'Int64', 'unified_collection_id': 'Int64' }
    df = df.astype(types)
    
    # Now convert everything to strings
    types = {
        'unified_song_id': 'str',
        'unified_artist_id': 'str',
        'unified_collection_id': 'str'
    }
    df = df.astype(types)
    
    return df

def create_artists(artist_files):

    # Read and concatenate all the files
    df = pd.concat([ read_file(fullfile) for fullfile in artist_files ])

    # Drop duplicates and reset index
    df = df.drop_duplicates(subset=['Unified Artist ID', 'Primary Artist ID']).reset_index(drop=True)
    
    # Rename columns
    cols_rename = {
        'Unified Artist ID': 'unified_artist_id',
        'Primary Artist ID': 'primary_artist_id',
        'Unified Artist Name': 'unified_artist_name',
        'Primary Artist Name': 'primary_artist_name'
    }
    df.rename(columns=cols_rename, inplace=True)
    
    df = df.astype({ 'unified_artist_id': 'str', 'primary_artist_id': 'str' })
    
    return df

def create_collections(collection_files):

    # Read and concatenate all the files
    df = pd.concat([ read_file(fullfile) for fullfile in collection_files ])

    # Drop duplciates and reset index
    df = df.drop_duplicates('Unified Collection ID').reset_index(drop=True)

    # Rename columns
    cols_rename = {
        'Unified Collection ID': 'unified_collection_id',
        'Barcodes': 'barcodes',
        'Unified Collection Name': 'name',
        'Barcode Name': 'barcode_name',
        'Barcode Release Date': 'release_date',
        'Collection Type': 'type',
        'Barcode Type': 'barcode_type'
    }
    df.rename(columns=cols_rename, inplace=True)
    
    # Now drop unnecessary columns
    cols_drop = ['barcodes', 'barcode_name', 'barcode_type']
    df.drop(columns=cols_drop, inplace=True)
    
    # Clean types
    df = df.astype({ 'unified_collection_id': 'str', 'name': 'str' })
    
    # Fill na values for release dates
    df['release_date'] = df['release_date'].fillna(dt.today().strftime('%m/%d/%Y'))
    
    # Drop duplicates along the unified collection ids just in case
    df = df.drop_duplicates(subset=['unified_collection_id']).reset_index(drop=True)
    
    return df

def create_songs(songs_files):

    # Read and concatenate all the files
    df = pd.concat([ read_file(fullfile) for fullfile in songs_files ])

    # Drop duplciates and reset index
    df = df.drop_duplicates(subset='ISRC').reset_index(drop=True)
    
    # Drop unnecessary columns
    cols_drop = [
        'BDS Song ID',
        'ISRC Title',
        'Creation Date (Unified Song ID)',
        'Creation Date (ISRC)'
    ]
    df.drop(columns=cols_drop, inplace=True)

    # Rename columns
    cols_rename = {
        'UnifiedSongid': 'unified_song_id',
        'ISRC': 'isrc',
        'UnifiedSong Title': 'title',
        'ISRC Release Date': 'release_date'
    }
    df.rename(columns=cols_rename, inplace=True)

    # Fill na values in release_date field with todays date
    today = dt.today().strftime('%m/%d/%Y')
    df['release_date'] = df['release_date'].fillna(today)
    
    # Fill na values in titles
    df['title'] = df['title'].fillna('')
    
    # Clean types
    df = df.astype({ 'unified_song_id': 'str', 'isrc': 'str' })
    
    return df

def install(db, df, table):

    """
        All the files are going to be installed the same way - by deleting
        the existing table, and uploading the new table. There are a few reasons
        for this, but the thing that made me decide to do this was when I found
        out that Brent Faiyaz's new project didn't show up in graphitti because
        it was originally marked as a single and that needed to be updated later.
    """

    db.execute(f'delete from {table}')
    db.big_insert(df, table)

class MappingTablePipeline(PipelineBase):

    def __init__(self):
        PipelineBase.__init__(self)

        # If the local mapping table folder doesn't exist, create it
        if not os.path.exists(MAPPING_TABLE_FOLDER):
            os.mkdir(MAPPING_TABLE_FOLDER)

    def init(self):

        # Raw file names
        SONG_METADATA_GLOBAL = 'Song_Metadata_Global.txt'
        SONG_METADATA_US = 'Song_Metadata_US.txt'
        SONG_METADATA_CAN = 'Song_Metadata_CAN.txt'
        SONG_MAPPING_US = 'Song_Mapping_US.txt'
        SONG_MAPPING_CAN = 'Song_Mapping_CAN.txt'
        SONG_MAPPING_GLOBAL = 'Song_Mapping_Global.txt'
        COLLECTION_METADATA_US = 'Collection_Metadata_US.txt'
        COLLECTION_METADATA_CAN = 'Collection_Metadata_CAN.txt'
        ARTIST_METADATA_US = 'Artist_Metadata_US.txt'
        ARTIST_METADATA_CAN = 'Artist_Metadata_CAN.txt'
        ARTIST_METADATA_GLOBAL = 'Artist_Metadata_Global.txt'

        # Remote fullfiles
        REMOTE_PATH = './'
        REMOTE_SONG_META_GLOBAL_FILENAME       = os.path.join(REMOTE_PATH, SONG_METADATA_GLOBAL)
        REMOTE_SONG_META_US_FILENAME           = os.path.join(REMOTE_PATH, SONG_METADATA_US)
        REMOTE_SONG_META_CAN_FILENAME          = os.path.join(REMOTE_PATH, SONG_METADATA_CAN)
        REMOTE_MAP_US_FILENAME                 = os.path.join(REMOTE_PATH, SONG_MAPPING_US)
        REMOTE_MAP_CAN_FILENAME                = os.path.join(REMOTE_PATH, SONG_MAPPING_CAN)
        REMOTE_MAP_GLOBAL_FILENAME             = os.path.join(REMOTE_PATH, SONG_MAPPING_GLOBAL)
        REMOTE_COLLECTION_META_US_FILENAME     = os.path.join(REMOTE_PATH, COLLECTION_METADATA_US)
        REMOTE_COLLECTION_META_CAN_FILENAME    = os.path.join(REMOTE_PATH, COLLECTION_METADATA_CAN)
        REMOTE_ARTIST_META_US_FILENAME         = os.path.join(REMOTE_PATH, ARTIST_METADATA_US)
        REMOTE_ARTIST_META_CAN_FILENAME        = os.path.join(REMOTE_PATH, ARTIST_METADATA_CAN)
        REMOTE_ARTIST_META_GLOBAL_FILENAME     = os.path.join(REMOTE_PATH, ARTIST_METADATA_GLOBAL)

        # Local fullfiles
        LOCAL_SONG_META_GLOBAL_FILENAME       = os.path.join(MAPPING_TABLE_FOLDER, SONG_METADATA_GLOBAL)
        LOCAL_SONG_META_US_FILENAME           = os.path.join(MAPPING_TABLE_FOLDER, SONG_METADATA_US)
        LOCAL_SONG_META_CAN_FILENAME          = os.path.join(MAPPING_TABLE_FOLDER, SONG_METADATA_CAN)
        LOCAL_MAP_US_FILENAME                 = os.path.join(MAPPING_TABLE_FOLDER, SONG_MAPPING_US)
        LOCAL_MAP_CAN_FILENAME                = os.path.join(MAPPING_TABLE_FOLDER, SONG_MAPPING_CAN)
        LOCAL_MAP_GLOBAL_FILENAME             = os.path.join(MAPPING_TABLE_FOLDER, SONG_MAPPING_GLOBAL)
        LOCAL_COLLECTION_META_US_FILENAME     = os.path.join(MAPPING_TABLE_FOLDER, COLLECTION_METADATA_US)
        LOCAL_COLLECTION_META_CAN_FILENAME    = os.path.join(MAPPING_TABLE_FOLDER, COLLECTION_METADATA_CAN)
        LOCAL_ARTIST_META_US_FILENAME         = os.path.join(MAPPING_TABLE_FOLDER, ARTIST_METADATA_US)
        LOCAL_ARTIST_META_CAN_FILENAME        = os.path.join(MAPPING_TABLE_FOLDER, ARTIST_METADATA_CAN)
        LOCAL_ARTIST_META_GLOBAL_FILENAME     = os.path.join(MAPPING_TABLE_FOLDER, ARTIST_METADATA_GLOBAL)

        # Local cleaned fullfiles
        cleaned_filename = lambda filename: filename.replace('.txt', '_cleaned.txt')
        LOCAL_CLEANED_SONG_META_GLOBAL_FILENAME       = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(SONG_METADATA_GLOBAL))
        LOCAL_CLEANED_SONG_META_US_FILENAME           = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(SONG_METADATA_US))
        LOCAL_CLEANED_SONG_META_CAN_FILENAME          = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(SONG_METADATA_CAN))
        LOCAL_CLEANED_MAP_US_FILENAME                 = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(SONG_MAPPING_US))
        LOCAL_CLEANED_MAP_CAN_FILENAME                = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(SONG_MAPPING_CAN))
        LOCAL_CLEANED_MAP_GLOBAL_FILENAME             = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(SONG_MAPPING_GLOBAL))
        LOCAL_CLEANED_COLLECTION_META_US_FILENAME     = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(COLLECTION_METADATA_US))
        LOCAL_CLEANED_COLLECTION_META_CAN_FILENAME    = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(COLLECTION_METADATA_CAN))
        LOCAL_CLEANED_ARTIST_META_US_FILENAME         = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(ARTIST_METADATA_US))
        LOCAL_CLEANED_ARTIST_META_CAN_FILENAME        = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(ARTIST_METADATA_CAN))
        LOCAL_CLEANED_ARTIST_META_GLOBAL_FILENAME     = os.path.join(MAPPING_TABLE_FOLDER, cleaned_filename(ARTIST_METADATA_GLOBAL))

        # Group the files into their different dimensions, we'll use this later for aggregating and uploading
        SONG_FILES = [ LOCAL_CLEANED_SONG_META_GLOBAL_FILENAME, LOCAL_CLEANED_SONG_META_US_FILENAME, LOCAL_CLEANED_SONG_META_CAN_FILENAME ]
        MAP_FILES = [ LOCAL_CLEANED_MAP_US_FILENAME, LOCAL_CLEANED_MAP_CAN_FILENAME, LOCAL_CLEANED_MAP_GLOBAL_FILENAME ]
        COLLECTION_FILES = [ LOCAL_CLEANED_COLLECTION_META_US_FILENAME, LOCAL_CLEANED_COLLECTION_META_CAN_FILENAME ]
        ARTIST_FILES = [ LOCAL_CLEANED_ARTIST_META_US_FILENAME, LOCAL_CLEANED_ARTIST_META_CAN_FILENAME, LOCAL_CLEANED_ARTIST_META_GLOBAL_FILENAME ]

        LOCAL_FILES = [
            LOCAL_SONG_META_GLOBAL_FILENAME,
            LOCAL_SONG_META_US_FILENAME,
            LOCAL_SONG_META_CAN_FILENAME,
            LOCAL_MAP_US_FILENAME,
            LOCAL_MAP_CAN_FILENAME,
            LOCAL_MAP_GLOBAL_FILENAME,
            LOCAL_COLLECTION_META_US_FILENAME,
            LOCAL_COLLECTION_META_CAN_FILENAME,
            LOCAL_ARTIST_META_US_FILENAME,
            LOCAL_ARTIST_META_CAN_FILENAME,
            LOCAL_ARTIST_META_GLOBAL_FILENAME
        ]

        REMOTE_FILES = [
            REMOTE_SONG_META_GLOBAL_FILENAME,
            REMOTE_SONG_META_US_FILENAME,
            REMOTE_SONG_META_CAN_FILENAME,
            REMOTE_MAP_US_FILENAME,
            REMOTE_MAP_CAN_FILENAME,
            REMOTE_MAP_GLOBAL_FILENAME,
            REMOTE_COLLECTION_META_US_FILENAME,
            REMOTE_COLLECTION_META_CAN_FILENAME,
            REMOTE_ARTIST_META_US_FILENAME,
            REMOTE_ARTIST_META_CAN_FILENAME,
            REMOTE_ARTIST_META_GLOBAL_FILENAME   
        ]

        LOCAL_CLEANED_FILES = [
            LOCAL_CLEANED_SONG_META_GLOBAL_FILENAME,
            LOCAL_CLEANED_SONG_META_US_FILENAME,
            LOCAL_CLEANED_SONG_META_CAN_FILENAME,
            LOCAL_CLEANED_MAP_US_FILENAME,
            LOCAL_CLEANED_MAP_CAN_FILENAME,
            LOCAL_CLEANED_MAP_GLOBAL_FILENAME,
            LOCAL_CLEANED_COLLECTION_META_US_FILENAME,
            LOCAL_CLEANED_COLLECTION_META_CAN_FILENAME,
            LOCAL_CLEANED_ARTIST_META_US_FILENAME,
            LOCAL_CLEANED_ARTIST_META_CAN_FILENAME,
            LOCAL_CLEANED_ARTIST_META_GLOBAL_FILENAME
        ]

        TOTAL_FILES_COUNT = len(LOCAL_FILES)

        self.TOTAL_FILES_COUNT = TOTAL_FILES_COUNT
        self.files = {
            'LOCAL_FILES': LOCAL_FILES,
            'REMOTE_FILES': REMOTE_FILES,
            'LOCAL_CLEANED_FILES': LOCAL_CLEANED_FILES,
            'SONG_FILES': SONG_FILES,
            'MAP_FILES': MAP_FILES,
            'COLLECTION_FILES': COLLECTION_FILES,
            'ARTIST_FILES': ARTIST_FILES
        }

    def finish(self):

        # Delete the raw files
        for fullfile in self.files['LOCAL_FILES']:
            os.remove(fullfile)

        # Delete the cleaned files
        for fullfile in self.files['LOCAL_CLEANED_FILES']:
            os.remove(fullfile)

        self.printSuccess(self.time.getElapsed())

def mapping_table(settings):

    print('Starting...')

    # Init the pipeline
    pipe = MappingTablePipeline()
    pipe.init()
    time = Time()

    # Download files from the remote sftp server
    download_files(pipe)
    pipe.printFnComplete(time.getElapsed('Files downloaded'))
    time.reset()

    # Generate the cleaned versions of the files
    clean_files(pipe)
    pipe.printFnComplete(time.getElapsed('Files cleaned'))
    time.reset()

    # Create db client
    db = Db('rca_db')
    db.connect()

    # For each dimension, read in the dataset and install it to the db
    print('Working on songs table...')
    songs_table = 'nielsen_map.songs'
    songs = create_songs(pipe.files['SONG_FILES'])
    print('Songs table generated...installing now')
    install(db, songs, songs_table)
    pipe.printFnComplete(time.getElapsed('Songs installed'))
    time.reset()

    print('Working on map...')
    nielsen_map_table = 'nielsen_map.map'
    nielsen_map = create_map(pipe.files['MAP_FILES'])
    print('Map table generated...installing now')
    install(db, nielsen_map, nielsen_map_table)
    pipe.printFnComplete(time.getElapsed('Map installed'))
    time.reset()

    print('Working on collections...')
    collections_table = 'nielsen_map.collections'
    collections = create_collections(pipe.files['COLLECTION_FILES'])
    print('Collections table generated...installing now')
    install(db, collections, collections_table)
    pipe.printFnComplete(time.getElapsed('Collections installed'))
    time.reset()

    print('Working on artists...')
    artists_table = 'nielsen_map.artists'
    artists = create_artists(pipe.files['ARTIST_FILES'])
    print('Artists table generated...installing now')
    install(db, artists, artists_table)
    pipe.printFnComplete(time.getElapsed('Artists installed'))
    time.reset()

    # Only commit changes if we aren't testing
    if settings['is_testing'] == False:
        db.commit()

    pipe.finish()