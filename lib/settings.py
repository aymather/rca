from .schedule import schedule
from datetime import datetime, timedelta
import argparse

def get_settings():

    """
        This function parses the command line inputs (or lack thereof)
        and builds the runtime settings.
    """

    # Get the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date')
    parser.add_argument('--testing')
    args = parser.parse_args()

    # Determie the date that we're running for
    if args.date is None:

        # If we don't specify, assume we're running the current day
        run_date = datetime.today()

    else:

        # If we do specify, then parse the date that we passed
        run_date = datetime.strptime(args.date, '%Y-%m-%d')

    # Get the day of the week from the date
    day_of_week = run_date.strftime('%A')

    # Determine the global data data, which is basically just
    # going to be -3 days from the date that we're running
    global_date_delta = 3
    if day_of_week == 'Monday':
        global_date_delta += 1
    global_date = run_date - timedelta(global_date_delta)

    # Determine wether or not we're testing
    if args.testing is None:

        # If we don't specify, assume we're running production
        is_testing = False

    else:

        # We can only pass 'True' or 'False'
        if args.testing not in ['True', 'False']:
            raise Exception('Invalid argument --testing, only accepts True or False')
        is_testing = True if args.testing == 'True' else False

    # Build the runtime settings
    settings = {
        'date': run_date,
        'global_date': global_date,
        'is_testing': is_testing
    }