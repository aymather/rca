from time import perf_counter as trackTime
from datetime import datetime
import math


class Time:

    def __init__(self):

        self.time = trackTime()

    def elapsed(self, msg=None):
            
        elapsed = trackTime() - self.time
        minutes = math.floor(elapsed / 60)
        seconds = round(elapsed % 60)
        
        if msg is None:
            print('Elapsed: {}m {}s'.format(minutes, seconds))
            return

        print('{} | Elapsed: {}m {}s'.format(msg, minutes, seconds))

    def getElapsed(self, msg=None):

        elapsed = trackTime() - self.time
        minutes = math.floor(elapsed / 60)
        seconds = round(elapsed % 60)

        if msg is None:
            return 'Elapsed: {}m {}s'.format(minutes, seconds)

        return '{} | Elapsed: {}m {}s'.format(msg, minutes, seconds)

    def reset(self):
        
        self.time = trackTime()