import pysftp
import paramiko
import subprocess
from .env import (
    RCA_NIELSEN_US_DAILY_SFTP_USERNAME, 
    RCA_NIELSEN_US_DAILY_SFTP_PASSWORD, 
    RCA_NIELSEN_EU_DAILY_SFTP_USERNAME, 
    RCA_NIELSEN_EU_DAILY_SFTP_PASSWORD, 
    RCA_NIELSEN_NE_ASIA_DAILY_SFTP_USERNAME, 
    RCA_NIELSEN_NE_ASIA_DAILY_SFTP_PASSWORD, 
    RCA_NIELSEN_SE_ASIA_DAILY_SFTP_USERNAME, 
    RCA_NIELSEN_SE_ASIA_DAILY_SFTP_PASSWORD, 
    RCA_NIELSEN_LATIN_AMERICA_DAILY_SFTP_USERNAME, 
    RCA_NIELSEN_LATIN_AMERICA_DAILY_SFTP_PASSWORD, 
    RCA_NIELSEN_EMERGING_DAILY_SFTP_USERNAME, 
    RCA_NIELSEN_EMERGING_DAILY_SFTP_PASSWORD
)


CONNECTIONS = {
    'nielsen_daily': {
        'host': 'sftp.mrc-data.com',
        'port': 22,
        'username': RCA_NIELSEN_US_DAILY_SFTP_USERNAME,
        'password': RCA_NIELSEN_US_DAILY_SFTP_PASSWORD
    },
    'eu_daily': {
        'host': 'sftp.mrc-data.com',
        'port': 22,
        'username': RCA_NIELSEN_EU_DAILY_SFTP_USERNAME,
        'password': RCA_NIELSEN_EU_DAILY_SFTP_PASSWORD
    },
    'ne_asia_daily': {
        'host': 'sftp.mrc-data.com',
        'port': 22,
        'username': RCA_NIELSEN_NE_ASIA_DAILY_SFTP_USERNAME,
        'password': RCA_NIELSEN_NE_ASIA_DAILY_SFTP_PASSWORD
    },
    'se_asia_daily': {
        'host': 'sftp.mrc-data.com',
        'port': 22,
        'username': RCA_NIELSEN_SE_ASIA_DAILY_SFTP_USERNAME,
        'password': RCA_NIELSEN_SE_ASIA_DAILY_SFTP_PASSWORD
    },
    'lat_am_daily': {
        'host': 'sftp.mrc-data.com',
        'port': 22,
        'username': RCA_NIELSEN_LATIN_AMERICA_DAILY_SFTP_USERNAME,
        'password': RCA_NIELSEN_LATIN_AMERICA_DAILY_SFTP_PASSWORD
    },
    'emerging_daily': {
        'host': 'sftp.mrc-data.com',
        'port': 22,
        'username': RCA_NIELSEN_EMERGING_DAILY_SFTP_USERNAME,
        'password': RCA_NIELSEN_EMERGING_DAILY_SFTP_PASSWORD
    }
}

CONNECTION_OPTIONS = pysftp.CnOpts(knownhosts=None)
CONNECTION_OPTIONS.hostkeys = None

class Sftp:

    def __init__(self, connection_name):
        self.set_connection(connection_name)

    def set_connection(self, connection_name):

        if connection_name not in CONNECTIONS:
            raise Exception('Not a valid connection name')

        self.connection_name = connection_name

    def get_config(self):
        return CONNECTIONS[self.connection_name]

    def connect(self):
        
        try:

            config = self.get_config()
            conn = pysftp.Connection(
                config['host'],
                username=config['username'],
                password=config['password'],
                port=config['port'],
                cnopts=CONNECTION_OPTIONS
            )
            print(f'Connected to sftp: {self.connection_name}')
            return conn

        except:

            config = self.get_config()
            conn = pysftp.Connection(
                config['host'],
                username=config['username'],
                password=config['password'],
                port=config['port'],
                cnopts=CONNECTION_OPTIONS
            )
            print(f'Connected to sftp: {self.connection_name}')
            return conn

    def list(self, path='.'):

        with self.connect() as sftp:
            return sftp.listdir(path)

            sftp.close()

    def get(self, remote_fullfile, local_fullfile):

        with self.connect() as sftp:
            print(f'Copying {remote_fullfile} to {local_fullfile}')
            sftp.get(remote_fullfile, local_fullfile)
            print('Copied successfully...')

            sftp.close()