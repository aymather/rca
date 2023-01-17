from .settings import get_settings
from abc import ABC, abstractmethod
from .PipelineBase import PipelineBase

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

    def set(self, day: str, pipeline: PipelineBase, error_on_failure: bool = False):

        self.schedule[day].append({
            'Pipeline': pipeline,
            'error_on_failure': error_on_failure
        })

    def run(self):
        day_of_week = self.settings['date'].strftime('%A')
        for pipeline in self.schedule[day_of_week]:
            
            Pipeline = pipeline['Pipeline']
            error_on_failure = pipeline['error_on_failure']

            pipe = Pipeline(self.db_name)
            
            print(f'Running pipeline: ' + pipe.__class__.__name__)
            
            try:

                pipe.run()

            except Exception as e:
                print('Exception on pipeline: ' + pipe.__class__.__name__)
                print(str(e))

                if error_on_failure:
                    raise e

    @abstractmethod
    def build(self):
        pass