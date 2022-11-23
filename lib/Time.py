from time import perf_counter as trackTime
import math


class Time:

    def __init__(self):

        self.time = trackTime()

    def printElapsed(self):
            
        elapsed = trackTime() - self.time
        minutes = math.floor(elapsed / 60)
        seconds = round(elapsed % 60)
        
        print('Elapsed: {}m {}s'.format(minutes, seconds))

    def getElapsed(self):

        elapsed = trackTime() - self.time
        minutes = math.floor(elapsed / 60)
        seconds = round(elapsed % 60)

        return '{}m {}s'.format(minutes, seconds)

    def reset(self):
        
        self.time = trackTime()