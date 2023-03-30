from .env import MAPPING_TABLE_FOLDER
from .PipelineBase import PipelineBase
from .Sftp import Sftp
from datetime import datetime
import pandas as pd
import os

GLOBAL_S3_UPLOAD_FOLDER_TEMPLATE = 'nielsen_archive/mapping_tables/{}/{}'

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

TOTAL_FILES_COUNT = TOTAL_FILES_COUNT
FILES_DICT = {
    'LOCAL_FILES': LOCAL_FILES,
    'REMOTE_FILES': REMOTE_FILES,
    'LOCAL_CLEANED_FILES': LOCAL_CLEANED_FILES,
    'SONG_FILES': SONG_FILES,
    'MAP_FILES': MAP_FILES,
    'COLLECTION_FILES': COLLECTION_FILES,
    'ARTIST_FILES': ARTIST_FILES
}

class NielsenWeeklyMappingTablePipeline(PipelineBase):

    def __init__(self, db_name):
        PipelineBase.__init__(self, db_name)

    def readFile(self, filename):

        """
            We do do a lot of file reading in here, and I just like to keep this
            uniform on account of the delimiters / encoding changes & idk nielsen
            is always just super annoying about changing how they deliver files to us.
        """

        return pd.read_csv(filename, delimiter='\t', on_bad_lines='skip')

    def buildMap(self):

        df = pd.concat([ self.readFile(fullfile) for fullfile in FILES_DICT['MAP_FILES'] ])

        df = df.drop_duplicates(subset=['UnifiedSongID', 'Unified Artist ID', 'Unified Collection ID']).reset_index(drop=True)

        cols_rename = {
            'UnifiedSongID': 'unified_song_id',
            'Unified Artist ID': 'unified_artist_id',
            'Unified Collection ID': 'unified_collection_id'
        }
        df.rename(columns=cols_rename, inplace=True)

        # We have to do type casting this way just cuz...
        df = df.astype({ 'unified_artist_id': 'Int64', 'unified_collection_id': 'Int64' })
        df = df.astype({
            'unified_song_id': 'str',
            'unified_artist_id': 'str',
            'unified_collection_id': 'str'
        })

        return df

    def buildArtists(self):

        df = pd.concat([ self.readFile(fullfile) for fullfile in FILES_DICT['ARTIST_FILES'] ])

        df = df.drop_duplicates(subset=['Unified Artist ID', 'Primary Artist ID']).reset_index(drop=True)

        cols_rename = {
            'Unified Artist ID': 'unified_artist_id',
            'Primary Artist ID': 'primary_artist_id',
            'Unified Artist Name': 'unified_artist_name',
            'Primary Artist Name': 'primary_artist_name'
        }
        df.rename(columns=cols_rename, inplace=True)

        df = df.astype({ 'unified_artist_id': 'str', 'primary_artist_id': 'str' })

        return df

    def buildCollections(self):

        df = pd.concat([ self.readFile(fullfile) for fullfile in FILES_DICT['COLLECTION_FILES'] ])

        df = df.drop_duplicates('Unified Collection ID').reset_index(drop=True)

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

        cols_drop = ['barcodes', 'barcode_name', 'barcode_type']
        df.drop(columns=cols_drop, inplace=True)

        df = df.astype({ 'unified_collection_id': 'str', 'name': 'str' })

        df['release_date'] = df['release_date'].fillna(datetime.today().strftime('%m/%d/%Y'))

        df = df.drop_duplicates(subset=['unified_collection_id']).reset_index(drop=True)

        return df

    def buildSongs(self):

        df = pd.concat([ self.readFile(fullfile) for fullfile in FILES_DICT['SONG_FILES'] ])

        df = df.drop_duplicates(subset='ISRC').reset_index(drop=True)

        cols_drop = [
            'BDS Song ID',
            'ISRC Title',
            'Creation Date (Unified Song ID)',
            'Creation Date (ISRC)'
        ]
        df.drop(columns=cols_drop, inplace=True)

        cols_rename = {
            'UnifiedSongid': 'unified_song_id',
            'ISRC': 'isrc',
            'UnifiedSong Title': 'title',
            'ISRC Release Date': 'release_date'
        }
        df.rename(columns=cols_rename, inplace=True)

        today = datetime.today().strftime('%m/%d/%Y')
        df['release_date'] = df['release_date'].fillna(today)

        df['title'] = df['title'].fillna('')
        
        df = df.astype({ 'unified_song_id': 'str', 'isrc': 'str' })

        return df

    def downloadFiles(self):

        sftp = Sftp('mapping_table')
        with sftp.connect() as conn:

            # Download each file
            for idx, (local_fullfile, remote_fullfile) in enumerate(zip(FILES_DICT['LOCAL_FILES'], FILES_DICT['REMOTE_FILES'])):

                # We're only going to download it if it doesn't already exist, the end of this script
                # removes all of these files so until then, they're in a "cached" state, which will be helpful
                # if we have to debug an error, but we don't want to have to redownload everything every time
                # we retry the script.
                if os.path.exists(local_fullfile):
                    print(f'Local file {local_fullfile} already exists...skipping...')
                    continue

                print(f'Downloading {idx + 1}/{TOTAL_FILES_COUNT}')
                conn.get(remote_fullfile, local_fullfile)

    def deleteFiles(self):

        # Delete the raw files
        for fullfile in FILES_DICT['LOCAL_FILES']:
            os.remove(fullfile)

        # Delete the cleaned files
        for fullfile in FILES_DICT['LOCAL_CLEANED_FILES']:
            os.remove(fullfile)

    def cleanFiles(self):

        # Clean each file
        for idx, (local_fullfile, local_cleaned_fullfile) in enumerate(zip(FILES_DICT['LOCAL_FILES'], FILES_DICT['LOCAL_CLEANED_FILES'])):

            # Only clean it if it hasn't already been cleaned, same as before, we'll delete it at the end
            # so until then it'll be in a "cached" state.
            if os.path.exists(local_cleaned_fullfile):
                print(f'File {local_cleaned_fullfile} already cleaned...skipping...')
                continue

            print(f'Cleaning {idx + 1}/{TOTAL_FILES_COUNT}')
            with open(local_fullfile, 'r') as f, open(local_cleaned_fullfile, 'w') as fo:
                for line in f:
                    fo.write(line.replace('\\', '').replace('"', ''))

    def archiveFiles(self):

        date = self.settings['date'].strftime('%Y-%m-%d')
        for idx, local_fullfile in enumerate(LOCAL_CLEANED_FILES):

            print(f'Archiving file {idx + 1}/{len(LOCAL_CLEANED_FILES)}')
            s3_fullfile = GLOBAL_S3_UPLOAD_FOLDER_TEMPLATE.format(date, os.path.basename(local_fullfile))
            self.aws.upload_s3(local_fullfile, s3_fullfile)

    def processArtists(self):

        df = self.buildArtists()

        if self.settings['is_testing'] == True:
            df = df.sample(1000).reset_index(drop=True)

        string = """
            delete from nielsen_map.artists;
        """
        self.db.execute(string)
        self.db.big_insert(df, 'nielsen_map.artists')

    def processCollections(self):

        df = self.buildCollections()

        if self.settings['is_testing'] == True:
            df = df.sample(1000).reset_index(drop=True)

        string = """
            delete from nielsen_map.collections;
        """
        self.db.execute(string)
        self.db.big_insert(df, 'nielsen_map.collections')

    def processMap(self):

        df = self.buildMap()

        if self.settings['is_testing'] == True:
            df = df.sample(1000).reset_index(drop=True)

        string = """
            delete from nielsen_map.map
        """
        self.db.execute(string)
        self.db.big_insert(df, 'nielsen_map.map')

    def processSongs(self):

        df = self.buildSongs()

        if self.settings['is_testing'] == True:
            df = df.sample(1000).reset_index(drop=True)

        string = """
            delete from nielsen_map.songs
        """
        self.db.execute(string)
        self.db.big_insert(df, 'nielsen_map.songs')

    def updateArtistTrackCount(self):

        string = """
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

            insert into nielsen_song.track_collections (song_id, unified_collection_id)
            select
                m.id,
                map.unified_collection_id
            from nielsen_song.meta m
            left join nielsen_map.map on m.unified_song_id = map.unified_song_id
            where map.unified_collection_id is not null
            group by m.id, map.unified_collection_id
            on conflict (song_id, unified_collection_id) do nothing
        """
        self.db.execute(string)

    def build(self):
        
        self.add_function(self.downloadFiles, 'Downloading Mapping Files')
        self.add_function(self.cleanFiles, 'Clean Mapping Files')

        self.add_function(self.processSongs, 'Process Songs')
        self.add_function(self.processArtists, 'Process Artists')
        self.add_function(self.processMap, 'Process Map')
        self.add_function(self.processCollections, 'Process Collections')
        
        self.add_function(self.updateArtistTrackCount, 'Update Artist Track Count')

        self.add_function(self.archiveFiles, 'Archiving Files')

        self.add_function(self.deleteFiles, 'Deleting Mapping Files')

    def test_build(self):
        
        self.add_function(self.downloadFiles, 'Downloading Mapping Files')
        self.add_function(self.cleanFiles, 'Clean Mapping Files')

        self.add_function(self.processSongs, 'Process Songs')
        self.add_function(self.processArtists, 'Process Artists')
        self.add_function(self.processMap, 'Process Map')
        self.add_function(self.processCollections, 'Process Collections')
        
        self.add_function(self.deleteFiles, 'Deleting Mapping Files')