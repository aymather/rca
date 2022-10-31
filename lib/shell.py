from .settings import get_settings
from .schedule import schedule

def main():

    """
        This function creates the runtime settings and
        runs our jobs.
    """

    settings = get_settings()
    schedule(settings)