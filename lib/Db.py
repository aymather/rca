from .env import TMP_FOLDER, RCA_DB, REPORTING_DB
from datetime import datetime
from psycopg2 import sql
import psycopg2.extras
import psycopg2
import pandas as pd
import os


# Numpy type int64 adapter
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

db_connections = {
    'rca_db': RCA_DB,
    'reporting_db': REPORTING_DB
}

class Db:

    def __init__(self, db_name='rca_db'):

        # If tmp folder doesn't exist, create it
        if os.path.isdir(TMP_FOLDER) == False:
            os.mkdir(TMP_FOLDER)

        self.db_name = db_name
        self.connection_string = db_connections[db_name]
        self.conn = None
        self.cur = None

    def test(self):

        """
            Simple method to test connection and query ability.
        """
        
        try:

            self.connect()
            self.execute('select 1')
            self.disconnect()
            print(f'Successful connection to: {self.db_name}')

        except Exception as e:
            raise Exception(f'Error testing db: {self.db_name}')

    def reset(self):
        
        self.disconnect()
        self.conn = None
        self.cur = None
        self.connect()

    def rollback(self):
        self.conn.rollback()

    def execute(self, string, params = {}):
        self.cur.execute(string, params)
        return self.df()

    def commit(self):
        self.conn.commit()

    def connect(self):

        self.conn = psycopg2.connect(self.connection_string)
        self.cur = self.conn.cursor()
        print(f'Connected to {self.db_name}...')

    def disconnect(self):

        if self.cur is not None:
            self.cur.close()

        if self.conn is not None:
            self.conn.close()
            print(f'Connection to {self.db_name} closed...')

    def cols(self):

        """
            Get an array of all the current column names in the cursor.
        """

        if self.cur.description is None:
            return None

        return [i[0] for i in self.cur.description]
    
    def df(self):

        """
            Take what's in the cursor and return it as a dataframe
            with the existing columns.
        """
        
        # Extract Columns
        cols = self.cols()

        if cols is None:
            return None

        # Extract data from cursor
        data = self.cur.fetchall()

        return pd.DataFrame(data, columns=cols)

    def big_insert(self, df, table, commit=False):

        """
            Wrapper method around the psycopg2 copy_expert method
            so that we can pass in a pandas dataframe.

            Quick method to copy data into the database that
            has a schema and table.
        """

        # Build query
        string = create_copy_expert_string(table, df.columns)

        # Copy to database
        self.copy_expert(df, string)

        # Commit changes if we specify it
        if commit is True:
            self.commit()

    def copy_expert(self, df, string):

        """
            This is similar to self.big_insert except it allows you
            to build your query string yourself so it's a bit more
            flexible, but requires a bit more work prior.
        """

        # Build the temporary filename
        csv_filename = f'tmp_df_{datetime.now()}.csv'
        csv_fullfile = os.path.join(TMP_FOLDER, csv_filename)

        # Write temporary file
        df.to_csv(csv_fullfile, index=False, na_rep='NaN')

        # Copy into table
        with open(csv_fullfile) as csv_file:
            self.cur.copy_expert(string, csv_file)

        # Remove temporary csv file
        os.remove(csv_fullfile)

    def __del__(self):
        self.disconnect()

    def __repr__(self):
        return f'<Db connected={self.conn is not None and self.cur is not None} />'


def create_copy_expert_string(table, columns=None):

    """
        This is just the standardized way of building the copy_expert strings.
    """

    if columns is not None:

        string = sql.SQL("""
            copy {} ({})
            from stdin (
                format csv,
                null "NaN",
                delimiter ',',
                header
            )
        """).format(sql.Identifier(*table.split('.')), sql.SQL(',').join([sql.Identifier(i) for i in columns]))
        return string

    string = sql.SQL("""
        copy {} 
        from stdin (
            format csv,
            null "NaN",
            delimiter ',',
            header
        )
    """).format(sql.Identifier(*table.split('.')))

    return string