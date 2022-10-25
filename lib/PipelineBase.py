from simple_chalk import chalk
from .Time import Time

class PipelineBase:

    def __init__(self):

        # Chalk settings
        self.stageColor = chalk.red
        self.fnCompleteColor = chalk.cyan
        self.successColor = chalk.green

        # Start timer
        self.time = Time()

    def printStage(self, msg):
        print(self.stageColor(msg))

    def printFnComplete(self, msg):
        print(self.fnCompleteColor(msg))

    def printSuccess(self, msg):
        print(self.successColor(msg))