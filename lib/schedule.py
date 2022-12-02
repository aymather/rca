from .global_pipeline import global_pipeline
from .pipeline import pipeline
from .mapping_table import mapping_table
from .monday_weekly import monday_weekly

def schedule(settings):

    """
        This function coordinates different pipelines that have different
        schedules. For example, our main pipeline runs every day except sunday,
        we have a few things that only run on monday, and the global data runs
        every day.
    """

    print(settings['day_of_week'])
    if settings['day_of_week'] == 'Sunday':

        print('Running: global pipeline')
        # global_pipeline(settings)

    elif settings['day_of_week'] == 'Monday':

        print('Running: Main pipeline, global pipeline, mapping table, monday weekly')
        pipeline(settings)
        # global_pipeline(settings)
        mapping_table(settings)
        monday_weekly(settings)

    elif settings['day_of_week'] == 'Tuesday':

        print('Running: Main pipeline, global pipeline')
        pipeline(settings)
        # global_pipeline(settings)

    elif settings['day_of_week'] == 'Wednesday':

        print('Running: Main pipeline, global pipeline')
        pipeline(settings)
        # global_pipeline(settings)

    elif settings['day_of_week'] == 'Thursday':

        print('Running: Main pipeline, global pipeline')
        pipeline(settings)
        # global_pipeline(settings)

    elif settings['day_of_week'] == 'Friday':

        print('Running: Main pipeline, global pipeline')
        pipeline(settings)
        # global_pipeline(settings)

    elif settings['day_of_week'] == 'Saturday':

        print('Running: Main pipeline, global pipeline')
        pipeline(settings)
        # global_pipeline(settings)