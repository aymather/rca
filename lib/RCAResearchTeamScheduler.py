from .SchedulerBase import SchedulerBase
from .NielsenDailyUSPipeline import NielsenDailyUSPipeline
from .NielsenDailyGlobalPipeline import NielsenDailyGlobalPipeline
from .NielsenWeeklyMappingTablePipeline import NielsenWeeklyMappingTablePipeline
from .WeeklyFunctionsPipeline import WeeklyFunctionsPipeline

class RCAResearchTeamScheduler(SchedulerBase):

    def __init__(self, db_name):
        SchedulerBase.__init__(self, db_name)

    def build(self):

        # Sunday
        self.set('Sunday', NielsenDailyGlobalPipeline)

        # Monday
        self.set('Monday', NielsenDailyUSPipeline)
        self.set('Monday', NielsenDailyGlobalPipeline)
        self.set('Monday', NielsenWeeklyMappingTablePipeline)
        self.set('Monday', WeeklyFunctionsPipeline)

        # Tuesday
        self.set('Tuesday', NielsenDailyUSPipeline)
        self.set('Tuesday', NielsenDailyUSPipeline)

        # Wednesday
        self.set('Wednesday', NielsenDailyUSPipeline)
        self.set('Wednesday', NielsenDailyUSPipeline)

        # Thursday
        self.set('Thursday', NielsenDailyUSPipeline)
        self.set('Thursday', NielsenDailyUSPipeline)

        # Friday
        self.set('Friday', NielsenDailyUSPipeline)
        self.set('Friday', NielsenDailyUSPipeline)

        # Saturday
        self.set('Saturday', NielsenDailyUSPipeline)
        self.set('Saturday', NielsenDailyUSPipeline)