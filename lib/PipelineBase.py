from .env import LOCAL_ARCHIVE_FOLDER, LOCAL_DOWNLOAD_FOLDER, TMP_FOLDER, REPORTS_FOLDER, MAPPING_TABLE_FOLDER
from simple_chalk import chalk
from .Time import Time
from abc import abstractmethod, ABC
from .settings import get_settings
from .Db import Db
from .Aws import Aws
from typing import Callable, List
import os

class PipelineBase(ABC):

    def __init__(self, db_name):

        # Make sure that our local working directories exist
        if not os.path.exists(LOCAL_ARCHIVE_FOLDER):
            os.makedirs(LOCAL_ARCHIVE_FOLDER)

        if not os.path.exists(LOCAL_DOWNLOAD_FOLDER):
            os.makedirs(LOCAL_DOWNLOAD_FOLDER)

        if not os.path.exists(TMP_FOLDER):
            os.makedirs(TMP_FOLDER)

        if not os.path.exists(REPORTS_FOLDER):
            os.makedirs(REPORTS_FOLDER)

        if not os.path.exists(MAPPING_TABLE_FOLDER):
            os.makedirs(MAPPING_TABLE_FOLDER)

        # This is the database we'll be updating, our postgres database
        # This is a parameter because we might be running data into our dev db or prod db
        # The self.settings['is_testing'] does NOT cause our pipelines to run on the dev db, the IMPLEMENTATION is what handles which db we use
        self.db_name = db_name
        self.db = Db(db_name)
        self.db.connect()

        # Chalk settings
        self.fnCompleteColor = chalk.cyan
        self.successColor = chalk.green

        # Basic pipeline settings
        # is_testing | tells us which build to use when we call self.run() - you can set up a test_build in the implementation
        # date | runtime date to use, this is typically just "today" but you can also use the --date argument to change which date you want to run
        self.settings = get_settings()

        # Each pipeline has a list of functions that can be run
        self.funcs: List[Callable] = []
        self.func_names: List[str] = []

        # For interacting with s3 mostly
        self.aws = Aws()
        self.aws.connect_s3()

    # Simple functions to print in colors our major events
    def printFnComplete(self, msg: str = '') -> None:
        print(self.fnCompleteColor(msg))

    def printSuccess(self, msg: str = '') -> None:
        print(self.successColor(msg))

    def add_function(self, func: Callable, name: str) -> None:
        self.funcs.append(func)
        self.func_names.append(name)

    # Entry point
    def run(self):

        """
            The implementation of this class must build the pipeline using
            the self.add_function method. Calling this method will run the
            series of functions built from that.
        """

        self.build() if self.settings['is_testing'] == False else self.test_build()

        pipelineTime = Time()
        number_of_functions = len(self.funcs)
        for idx, (func, name) in enumerate(zip(self.funcs, self.func_names)):

            print(f'{idx}/{number_of_functions - 1} | Running function: {name}')
            fnTime = Time()
            func()
            self.printFnComplete(name + ': ' + fnTime.getElapsed())

        self.commit()
        self.db.disconnect()

        # Print the finished time
        self.printSuccess(f'{self.__class__.__name__} success: {pipelineTime.getElapsed()}')

    def commit(self):
        
        """
            Commit changes to the database if we aren't in testing mode.
        """

        self.db.commit() if self.settings['is_testing'] == False else self.db.rollback()

    @abstractmethod
    def build(self):
        pass

    @abstractmethod
    def test_build(self):
        pass