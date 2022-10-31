from .env import LOCAL_ARCHIVE_FOLDER, LOCAL_DOWNLOAD_FOLDER, REPORTS_FOLDER
from .PipelineBase import PipelineBase
from .Sftp import Sftp
from datetime import datetime
from zipfile import ZipFile
import os

NIELSEN_US_DAILY_ARCHIVE_FOLDER = '/' # location on nielsen's remote sftp server where the US daily files are located
NIELSEN_US_DAILY_ZIP_FILENAME = 'RCA_AR_Daily_Report_{}.zip'
NIELSEN_US_DAILY_ARTIST_FILENAME = 'RCA_AR_Artist_ODA_{}.csv'
NIELSEN_US_DAILY_SONG_FILENAME_OLD = 'RCA_AR_SONG_ODA_{}.csv'
NIELSEN_US_DAILY_SONG_FILENAME = 'RCA_AR_SONG_ODA_{}_New.csv'
EXPORTS_TEMPLATE = '{}_exports'
MAC_FOLDER = '__MACOSX'

DEFAULT_RUNTIME_SETTINGS = {
    'date': datetime.today(),
    'is_testing': True
}

class PipelineManager(PipelineBase):

    def __init__(self):
        PipelineBase.__init__(self)

        # Make folders if they don't already exist
        if os.path.isdir(LOCAL_ARCHIVE_FOLDER) == False:
            os.mkdir(LOCAL_ARCHIVE_FOLDER)

        if os.path.isdir(LOCAL_DOWNLOAD_FOLDER) == False:
            os.mkdir(LOCAL_DOWNLOAD_FOLDER)

        if os.path.isdir(REPORTS_FOLDER) == False:
            os.mkdir(REPORTS_FOLDER)

        self.date = None
        self.files = None
        self.fullfiles = None
        self.folders = None

    def init(self, settings=DEFAULT_RUNTIME_SETTINGS):

        """
            @param date | The date on the zip file that we need to process w/ format YYYYMMDD

            Collects all local filenames and fullfiles based on date passed so we can easily
            use these anywhere in the pipeline.
        """

        # Set our runtime settings
        self.settings = settings
        self.date = settings['date']

        # Format date to fit the report name
        formatted_date = datetime.strftime(self.date, format='%Y%m%d')

        # Filenames
        zip_filename = NIELSEN_US_DAILY_ZIP_FILENAME.format(formatted_date)
        old_song_filename = NIELSEN_US_DAILY_SONG_FILENAME_OLD.format(formatted_date)
        song_filename = NIELSEN_US_DAILY_SONG_FILENAME.format(formatted_date)
        artist_filename = NIELSEN_US_DAILY_ARTIST_FILENAME.format(formatted_date)

        # Fullfiles
        zip_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, zip_filename)
        zip_remote_archive_fullfile = os.path.join(NIELSEN_US_DAILY_ARCHIVE_FOLDER, zip_filename)
        zip_local_archive_fullfile = os.path.join(LOCAL_ARCHIVE_FOLDER, zip_filename)
        old_song_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, NIELSEN_US_DAILY_SONG_FILENAME_OLD.format(formatted_date))
        song_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, NIELSEN_US_DAILY_SONG_FILENAME.format(formatted_date))
        artist_fullfile = os.path.join(LOCAL_DOWNLOAD_FOLDER, NIELSEN_US_DAILY_ARTIST_FILENAME.format(formatted_date))

        self.files = {
            'zip': zip_filename,
            'old_song': old_song_filename,
            'song': song_filename,
            'artist': artist_filename
        }

        self.fullfiles = {
            'zip': zip_fullfile,
            'zip_local_archive': zip_local_archive_fullfile,
            'zip_remote_archive': zip_remote_archive_fullfile,
            'old_song': old_song_fullfile,
            'song': song_fullfile,
            'artist': artist_fullfile
        }

        self.folders = {
            'exports': os.path.join(REPORTS_FOLDER, EXPORTS_TEMPLATE.format(formatted_date))
        }

        # Only download if some file is missing
        if os.path.exists(self.fullfiles['artist']) == False or os.path.exists(self.fullfiles['song']) == False:

            # Init SFTP client
            sftp = Sftp('nielsen_daily')

            # Download remote zip file to our local download folder
            sftp.get(self.fullfiles['zip_remote_archive'], self.fullfiles['zip'])

            # Unzip
            with ZipFile(self.fullfiles['zip'], 'r') as file_ref:
                file_ref.extractall(LOCAL_DOWNLOAD_FOLDER)

            # We can delete the old song file
            os.remove(self.fullfiles['old_song'])

            # Move the zip files into the archive (don't do this unless we have the required files)
            os.rename(self.fullfiles['zip'], self.fullfiles['zip_local_archive'])

            # Remove this annoying folder that sometimes comes from our zip files
            if os.path.isdir(os.path.join(LOCAL_DOWNLOAD_FOLDER, MAC_FOLDER)):
                os.rmdir(MAC_FOLDER)

            # Create an exports directory
            if os.path.isdir(self.folders['exports']) == False:
                os.mkdir(self.folders['exports'])

            print(f"Initialized file: {self.fullfiles['zip']}")

    def finish(self, db):

        """
            Delete files / clean up environment.
            Commit all our database changes if we aren't in testing mode
        """

        # Commit our db changes if we aren't in testing mode
        if self.settings['is_testing'] == False:
            db.commit()
        else:
            # If we're just testing, rollback changes
            db.rollback()

        db.disconnect()

        # Delete artist file
        if os.path.exists(self.fullfiles['artist']):
            os.remove(self.fullfiles['artist'])

        # Delete song file
        if os.path.exists(self.fullfiles['song']):
            os.remove(self.fullfiles['song'])

        self.files = None
        self.fullfiles = None
        self.date = None

        # Print the finished time
        print(self.successColor(self.time.getElapsed()))