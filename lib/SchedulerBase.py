from .settings import get_settings
from abc import ABC, abstractmethod

class SchedulerBase(ABC):

    def __init__(self, db_name):
        self.settings = get_settings()
        self.db_name = db_name
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

    def add_schedule(self, day_of_week, pipelines):
        self.schedule[day_of_week] = pipelines

    def run(self):
        day_of_week = self.settings['date'].strftime('%A')
        for Pipeline in self.schedule[day_of_week]:
            
            pipe = Pipeline(self.db_name)
            
            try:

                pipe.run()

            except Exception as e:
                print('Exception on pipeline: ' + pipe.__class__.__name__)
                print(str(e))

    @abstractmethod
    def build(self):
        pass