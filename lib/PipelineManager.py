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


class PipelineManager(PipelineBase):

    def __init__(self):
        PipelineBase.__init__(self)

        

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

        # Delete artist file
        if os.path.exists(self.fullfiles['artist']):
            os.remove(self.fullfiles['artist'])

        # Delete song file
        if os.path.exists(self.fullfiles['song']):
            os.remove(self.fullfiles['song'])

        