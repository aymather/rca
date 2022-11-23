from .PipelineBase import PipelineBase
from abc import ABC, abstractmethod

class SchedulerBase(ABC):

    def __init__(self):
        self.pipelines = []

    def add_pipeline(self, pipeline: PipelineBase):
        self.pipelines.append(pipeline)

    def run(self):
        for p in self.pipelines: p.run()

    @abstractmethod
    def create_schedule(self):
        pass

class Scheduler(SchedulerBase):

    def __init__(self):
        SchedulerBase.__init__(self)

    