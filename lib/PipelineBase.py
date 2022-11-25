from simple_chalk import chalk
from .Time import Time
from abc import abstractmethod, ABC
from .settings import get_settings
from .Db import Db
from typing import Callable, List

class PipelineBase(ABC):

    def __init__(self, db_name):

        self.db_name = db_name

        # Chalk settings
        self.fnCompleteColor = chalk.cyan
        self.successColor = chalk.green

        # Basic pipeline settings
        self.settings = get_settings()

        # Database
        self.db = Db(db_name)
        self.db.connect()

        # Each pipeline has a list of functions that can be run
        self.funcs: List[Callable] = []
        self.func_names: List[str] = []

    def printFnComplete(self, msg: str = '') -> None:
        print(self.fnCompleteColor(msg))

    def printSuccess(self, msg: str = '') -> None:
        print(self.successColor(msg))

    def add_function(self, func: Callable, name: str) -> None:
        self.funcs.append(func)
        self.func_names.append(name)

    def run(self):

        """
            Run the pipeline which is built from the self.build function
        """

        # Build the pipeline depending on whether we're testing or not
        if self.settings['is_testing'] == True:
            self.test_build()
        else:
            self.build()

        pipelineTime = Time()
        for func, name in zip(self.funcs, self.func_names):

            print(f'Running function: {name}')
            fnTime = Time()
            func()
            self.printFnComplete(name + ': ' + fnTime.getElapsed())

        self.commit()

        # Print the finished time
        self.printSuccess(f'Pipeline {self.__class__.__name__} success: {pipelineTime.getElapsed()}')

    def commit(self):
        
        """
            Commit changes to the database if we aren't in testing mode.
        """
        
        if self.settings['is_testing'] == True:
            self.db.rollback()
        else:
            self.db.commit()

    @abstractmethod
    def build(self):
        pass

    @abstractmethod
    def test_build(self):
        pass