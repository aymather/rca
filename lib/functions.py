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