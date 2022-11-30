# Pipelines
1. Nielsen Daily US Pipeline
    - This processes the daily nielsen data.
    - Runs every day except Sundays.
2. Nielsen Mapping Tables
    - This processes the mapping tables we get from nielsen.
    - Runs on mondays (that's when those tables are refreshed on the server)
3. Nielsen Daily Global Pipeline (Beta)
    - Processes the global datasets.
    - Runs every day.
4. Daily tasks
    - This is for tasks that we just need to run every day regardless of the nielsen data. For example, we gather spotify chart data every day that does not depend on
        the nielsen data having been processed.
    - Runs every day.
5. Weekly tasks
    - These are just refresh functions that need to be run once a week.
    - Runs every monday.