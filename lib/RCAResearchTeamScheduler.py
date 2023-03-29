from .SchedulerBase import SchedulerBase
from .NielsenDailyUSPipeline import NielsenDailyUSPipeline
from .NielsenDailyGlobalPipeline import NielsenDailyGlobalPipeline
from .NielsenWeeklyMappingTablePipeline import NielsenWeeklyMappingTablePipeline
from .WeeklyFunctionsPipeline import WeeklyFunctionsPipeline
from .NielsenWeeklyGlobalPipeline import NielsenWeeklyGlobalPipeline

class RCAResearchTeamScheduler(SchedulerBase):

    def __init__(self, db_name):
        SchedulerBase.__init__(self, db_name)

    def build(self):

        # Sunday
        self.set('Sunday', NielsenDailyGlobalPipeline)
        self.set('Sunday', NielsenWeeklyGlobalPipeline)

        # Monday
        self.set('Monday', NielsenDailyUSPipeline)
        self.set('Monday', NielsenDailyGlobalPipeline)
        self.set('Monday', NielsenWeeklyMappingTablePipeline)
        self.set('Monday', NielsenWeeklyGlobalPipeline)
        self.set('Monday', WeeklyFunctionsPipeline)

        # Tuesday
        self.set('Tuesday', NielsenDailyUSPipeline)
        self.set('Tuesday', NielsenDailyGlobalPipeline)
        self.set('Tuesday', NielsenWeeklyGlobalPipeline)

        # Wednesday
        self.set('Wednesday', NielsenDailyUSPipeline)
        self.set('Wednesday', NielsenDailyGlobalPipeline)
        self.set('Wednesday', NielsenWeeklyGlobalPipeline)

        # Thursday
        self.set('Thursday', NielsenDailyUSPipeline)
        self.set('Thursday', NielsenDailyGlobalPipeline)
        self.set('Thursday', NielsenWeeklyGlobalPipeline)

        # Friday
        self.set('Friday', NielsenDailyUSPipeline)
        self.set('Friday', NielsenDailyGlobalPipeline)
        self.set('Friday', NielsenWeeklyGlobalPipeline)

        # Saturday
        self.set('Saturday', NielsenDailyUSPipeline)
        self.set('Saturday', NielsenDailyGlobalPipeline)
        self.set('Saturday', NielsenWeeklyGlobalPipeline)

    def test_build(self):

        self.setToday(NielsenDailyUSPipeline)