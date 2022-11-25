from .PipelineBase import PipelineBase
from abc import ABC, abstractmethod

class SchedulerBase(ABC):

    def __init__(self):
        self.pipelines = []
        self.schedule = {
            'Monday': [],
            'Tuesday': [],
            'Wednesday': [],
            'Thursday': [],
            'Friday': [],
            'Saturday': [],
            'Sunday': []
        }

    def set(self, day, func):
        self.schedule[day].append(func)

    def add_pipeline(self, pipeline: PipelineBase):
        self.pipelines.append(pipeline)

    def run(self):
        for p in self.pipelines: p.run()

    @abstractmethod
    def build(self):
        pass

class Scheduler(SchedulerBase):

    def __init__(self):
        SchedulerBase.__init__(self)

    