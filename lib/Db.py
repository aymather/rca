import os
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import AsIs, register_adapter

from .Aws import Aws
from .env import (AWS_ACCESS_KEY, AWS_SECRET_KEY, RCA_DB_DEV, RCA_DB_PROD,
                  REPORTING_DB, TMP_FOLDER)


# Numpy type int64 adapter
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)

db_connections = {
    'rca_db_prod': RCA_DB_PROD,
    'rca_db_dev': RCA_DB_DEV,
    'reporting_db': REPORTING_DB
}


class Db:

    def __init__(self, db_name='rca_db_prod'):

        if db_name not in db_connections.keys():
            raise Exception(f'{db_name} is not a valid database connection name')

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
            print(str(e))
            raise Exception(f'Error testing db: {self.db_name}')

    def reset(self):
        
        self.disconnect()
        self.conn = None
        self.cur = None
        self.connect()

    def rollback(self):

        if self.conn is None:
            raise Exception(f'Connection to {self.db_name} not active.')

        self.conn.rollback()

    def execute(self, string, params = {}):

        if self.cur is None:
            raise Exception(f'Cursor to {self.db_name} not active.')

        self.cur.execute(string, params)
        return self.df()

    def commit(self):

        if self.conn is None:
            raise Exception(f'Connection to {self.db_name} not active.')

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

        if self.cur is None:
            raise Exception(f'Cursor to {self.db_name} not active.')

        if self.cur.description is None:
            return None

        return [i[0] for i in self.cur.description]
    
    def df(self):

        """
            Take what's in the cursor and return it as a dataframe
            with the existing columns.
        """

        if self.cur is None:
            raise Exception(f'Cursor to {self.db_name} not active.')
        
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

    def big_insert_redshift(self, df, table):

        """

            IMPORTANT:
            This method will only work when the connection provided for
            this database instance is for a redshift database. We currently
            have 2 different db types available as connection types, our personal
            postgres and the reporting db redshift. So this method won't work properly
            with our personal postgres.
        
            This is the same idea as self.big_insert except we have
            to do things a bit differently when inserting to a redshift db.

            First we have to write the file to a csv locally, then we have
            to upload it to s3, then we have to copy it into the redshift
            from the s3 bucket.

        """

        # We cannot upload anything with single/double quotes because those are reserved, so
        # we must clean the dataframe of those
        df = df[df.columns].replace({ "'": '', '"': '' }, regex=True)

        # Build files/folders
        csv_filename = f'tmp_df_{datetime.now()}.csv'
        local_csv_fullfile = os.path.join(TMP_FOLDER, csv_filename)
        remote_csv_fullfile = os.path.join('tmp', csv_filename)

        # Write the temporary file
        df.to_csv(local_csv_fullfile, index=False, na_rep='NaN')

        # Upload the file to the s3 bucket
        aws = Aws()
        aws.connect_s3()
        aws.upload_s3(local_csv_fullfile, remote_csv_fullfile)

        string_options = """
            from 's3://busd-rca-projects/{}'
            credentials 'aws_access_key_id={};aws_secret_access_key={}'
            ignoreheader 1
            delimiter ','
            emptyasnull
            escape removequotes
        """.format(
            remote_csv_fullfile,
            AWS_ACCESS_KEY,
            AWS_SECRET_KEY
        )

        # Copy the file from the s3 bucket over to the redshift
        string = sql.SQL('copy {} ({})\n' + string_options).format(
            sql.Identifier(*table.split('.')),
            sql.SQL(',').join([sql.Identifier(i) for i in df.columns])
        )

        self.execute(string)

        # Delete the file from the s3 bucket
        aws.delete_file_s3(remote_csv_fullfile)

        # Delete the local file
        os.remove(local_csv_fullfile)

        print('Copied successfully')

    def copy_expert(self, df, string):

        """
            This is similar to self.big_insert except it allows you
            to build your query string yourself so it's a bit more
            flexible, but requires a bit more work prior.
        """

        if self.cur is None:
            raise Exception(f'Cursor to {self.db_name} not active.')

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