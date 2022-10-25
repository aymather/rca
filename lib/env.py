from dotenv import load_dotenv
import os

load_dotenv()


# Load our environment variables with default values
LOCAL_ARCHIVE_FOLDER                                     = os.getenv('LOCAL_ARCHIVE_FOLDER')                                        or './archive'                                  # where we archive zip files after they're processed
LOCAL_DOWNLOAD_FOLDER                                    = os.getenv('LOCAL_DOWNLOAD_FOLDER')                                       or './nielsen_downloads'                        # this is where we download the zip files to
TMP_FOLDER                                               = os.getenv('TMP_FOLDER')                                                  or './tmp'                                      # temporary files
REPORTS_FOLDER                                           = os.getenv('REPORTS_FOLDER')                                              or './reports'                                  # folder of our pipeline's exports
MAPPING_TABLE_FOLDER                                     = os.getenv('MAPPING_TABLE_FOLDER')                                        or './mapping_table'                            # folder to store mapping table data
ENV_NAME                                                 = os.getenv('ENV_NAME')                                                                                                    # just the name of the environment so we know where we are
RCA_DB                                                   = os.getenv('RCA_DB')                                                                                                      # connection string to the rca postgres db
REPORTING_DB                                             = os.getenv('REPORTING_DB')                                                                                                # connection string to the sony reporting db
RCA_NIELSEN_US_DAILY_SFTP_USERNAME                       = os.getenv('RCA_NIELSEN_US_DAILY_SFTP_USERNAME')                                                                          # username to the nielsen sftp server for the daily US data
RCA_NIELSEN_US_DAILY_SFTP_PASSWORD                       = os.getenv('RCA_NIELSEN_US_DAILY_SFTP_PASSWORD')                                                                          # password to the nielsen sftp server for the daily US data
RCA_NIELSEN_MAPPING_TABLE_USERNAME                       = os.getenv('RCA_NIELSEN_MAPPING_TABLE_USERNAME')                                                                          # username for mapping table server
RCA_NIELSEN_MAPPING_TABLE_PASSWORD                       = os.getenv('RCA_NIELSEN_MAPPING_TABLE_PASSWORD')                                                                          # password for mapping table server
RCA_NIELSEN_EU_DAILY_SFTP_USERNAME                       = os.getenv('RCA_NIELSEN_EU_DAILY_SFTP_USERNAME')                                                                          # global data credentials
RCA_NIELSEN_EU_DAILY_SFTP_PASSWORD                       = os.getenv('RCA_NIELSEN_EU_DAILY_SFTP_PASSWORD')                                                                          # global data credentials
RCA_NIELSEN_NE_ASIA_DAILY_SFTP_USERNAME                  = os.getenv('RCA_NIELSEN_NE_ASIA_DAILY_SFTP_USERNAME')                                                                     # global data credentials
RCA_NIELSEN_NE_ASIA_DAILY_SFTP_PASSWORD                  = os.getenv('RCA_NIELSEN_NE_ASIA_DAILY_SFTP_PASSWORD')                                                                     # global data credentials
RCA_NIELSEN_SE_ASIA_DAILY_SFTP_USERNAME                  = os.getenv('RCA_NIELSEN_SE_ASIA_DAILY_SFTP_USERNAME')                                                                     # global data credentials
RCA_NIELSEN_SE_ASIA_DAILY_SFTP_PASSWORD                  = os.getenv('RCA_NIELSEN_SE_ASIA_DAILY_SFTP_PASSWORD')                                                                     # global data credentials
RCA_NIELSEN_LATIN_AMERICA_DAILY_SFTP_USERNAME            = os.getenv('RCA_NIELSEN_LATIN_AMERICA_DAILY_SFTP_USERNAME')                                                               # global data credentials
RCA_NIELSEN_LATIN_AMERICA_DAILY_SFTP_PASSWORD            = os.getenv('RCA_NIELSEN_LATIN_AMERICA_DAILY_SFTP_PASSWORD')                                                               # global data credentials
RCA_NIELSEN_EMERGING_DAILY_SFTP_USERNAME                 = os.getenv('RCA_NIELSEN_EMERGING_DAILY_SFTP_USERNAME')                                                                    # global data credentials
RCA_NIELSEN_EMERGING_DAILY_SFTP_PASSWORD                 = os.getenv('RCA_NIELSEN_EMERGING_DAILY_SFTP_PASSWORD')                                                                    # global data credentials