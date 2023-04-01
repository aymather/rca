from .env import LOCAL_ARCHIVE_FOLDER, LOCAL_DOWNLOAD_FOLDER, TMP_FOLDER, REPORTS_FOLDER, MAPPING_TABLE_FOLDER
from simple_chalk import chalk
from .Time import Time
from .Email import Email
from abc import abstractmethod, ABC
from .settings import get_settings
from .Db import Db
from .Aws import Aws
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

        # Just stores the day of the week, we have some stuff that depends on what day of the week it is, such as which reports
        # to generate and send out on which days
        self.day_of_week = self.settings['date'].strftime('%A')

        # Each pipeline has a list of functions that can be run
        self.functions = []

        # For sending emails
        self.email = Email()

        # Every time we run our pipeline, the return of that pipeline will tell us everything that happened
        # which we can then send to ourselves in an email to keep track of any errors or anything unexpected that happened in our pipeline.
        # Each item in this array will represent a function. If it completed successfully, it will just return a success message with the function
        # name, but if it failed it will return the error message with the function name.
        # Each function report will have the following structure.
        """
            {
                "name": <function name>,
                "success": <boolean>,
                "error": <error message if an error occured, if not then None>
            }
        """
        self.report = []

        # For interacting with s3 mostly
        self.aws = Aws()
        self.aws.connect_s3()

    # Simple functions to print in colors our major events
    def printFnComplete(self, msg):
        print(self.fnCompleteColor(msg))

    def printSuccess(self, msg):
        print(self.successColor(msg))

    def add_function(self, func, name, error_on_failure = True):

        """
            Function object outline:

            {
                "func": <Callable function to run>,
                "name": <Name of function>,
                "error_on_failure": <Boolean indicating whether the whole program should exit should this function error out>
            }
        """

        self.functions.append({
            'func': func,
            'name': name,
            'error_on_failure': error_on_failure
        })

    def add_report(self, name, success, error):
        self.report.append({
            'name': name,
            'success': success,
            'error': error
        })

    def get_report(self):

        """
            Converts the array of reports into a single string
            so that we can easily email it n stuff.
        """

        if len(self.report) == 0:
            return 'Nothing to report'
        
        report = ''
        for r in self.report:
            
            if r['success'] == True:
                report += r['name'] + ' success\n'
            else:
                report += '\n'
                report += r['name'] + ' error:\n'
                report += r['error']
                report += '\n'

        return report

    # Entry point
    def run(self):

        """
            The implementation of this class must build the pipeline using
            the self.add_function method. Calling this method will run the
            series of functions built from that.
        """

        self.build() if self.settings['is_testing'] == False else self.test_build()

        pipelineTime = Time()
        number_of_functions = len(self.functions)
        
        try:

            for idx, function in enumerate(self.functions):

                name = function['name']
                func = function['func']
                error_on_failure = function['error_on_failure']

                print(f'{idx}/{number_of_functions - 1} | Running function: {name}')
                fnTime = Time()
                
                try:

                    func()
                    self.add_report(name, True, '')

                except Exception as e:
                    print(str(e))
                    self.add_report(name, False, str(e))
                    if error_on_failure:
                        raise e

                self.printFnComplete(name + ': ' + fnTime.getElapsed())

            self.commit()
            self.db.disconnect()

            # Print the finished time
            self.printSuccess(f'{self.__class__.__name__} success: {pipelineTime.getElapsed()}')

        finally:

            # Even if our pipeline exits prematurely, we still want to return the reporting to be handled
            # by our scheduler
            return self.get_report()

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