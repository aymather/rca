from .SchedulerBase import SchedulerBase
from .NielsenDailyUSPipeline import NielsenDailyUSPipeline
from .NielsenDailyGlobalPipeline import NielsenDailyGlobalPipeline
from .NielsenWeeklyMappingTablePipeline import NielsenWeeklyMappingTablePipeline
from .WeeklyFunctionsPipeline import WeeklyFunctionsPipeline

class RCAResearchTeamScheduler(SchedulerBase):

    def __init__(self, db_name):
        SchedulerBase.__init__(self, db_name)

    def build(self):
        
        sunday = [NielsenDailyGlobalPipeline]
        monday = [NielsenDailyUSPipeline, NielsenDailyGlobalPipeline, NielsenWeeklyMappingTablePipeline, WeeklyFunctionsPipeline]
        tuesday = [NielsenDailyUSPipeline, NielsenDailyGlobalPipeline]
        wednesday = [NielsenDailyUSPipeline, NielsenDailyGlobalPipeline]
        thursday = [NielsenDailyUSPipeline, NielsenDailyGlobalPipeline]
        friday = [NielsenDailyUSPipeline, NielsenDailyGlobalPipeline]
        saturday = [NielsenDailyUSPipeline, NielsenDailyGlobalPipeline]

        self.add_schedule('Sunday', sunday)
        self.add_schedule('Monday', monday)
        self.add_schedule('Tuesday', tuesday)
        self.add_schedule('Wednesday', wednesday)
        self.add_schedule('Thursday', thursday)
        self.add_schedule('Friday', friday)
        self.add_schedule('Saturday', saturday)