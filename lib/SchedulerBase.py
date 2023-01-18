from .settings import get_settings
from abc import ABC, abstractmethod
from .PipelineBase import PipelineBase
from .Email import Email

DAILY_REPORT_RECIPIENTS = [
    'alec.mather@rcarecords.com'
]

class SchedulerBase(ABC):

    def __init__(self, db_name, should_email_report=True):
        self.settings = get_settings()
        self.db_name = db_name
        self.should_email_report = should_email_report
        self.email = Email()
        self.report = ''
        self.schedule = {
            'Monday': [],
            'Tuesday': [],
            'Wednesday': [],
            'Thursday': [],
            'Friday': [],
            'Saturday': [],
            'Sunday': []
        }

    def set(self, day: str, pipeline: PipelineBase):
        self.schedule[day].append(pipeline)

    def set_report(self, name: str, report: str):
        self.report += name + ' Report: \n'
        self.report += report
        self.report += '\n\n'

    def email_report(self):

        if self.should_email_report == False:
            return

        report_body = 'Daily report,\n\n' + self.report

        self.email.send(DAILY_REPORT_RECIPIENTS, 'Daily RCA Pipeline', report_body)

    def run(self):

        self.build()

        day_of_week = self.settings['date'].strftime('%A')
        for Pipeline in self.schedule[day_of_week]:

            pipe = Pipeline(self.db_name)
            print(f'Running pipeline: ' + pipe.__class__.__name__)
            
            report = pipe.run()
            self.set_report(pipe.__class__.__name__, report)

        self.email_report()

    @abstractmethod
    def build(self):
        pass