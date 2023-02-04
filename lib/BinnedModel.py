from typing import Tuple
from .Db import Db
import matplotlib.pyplot as plt
from psycopg2 import sql
import pandas as pd
import numpy as np


class BinnedModel:

    """
    
        This class is basically just a wrapper around the result of the pandas
        .describe() method which we save after completing our big data study on
        socials and how they change week over week.

        You must also save 'bin_min' and 'bin_max' as columns in that dataframe
        so that we can find what bin each input falls into.

    """

    def __init__(self, name: str):

        self.name = name
        self.model = self.fetch_model(name)
        self.max_bin = self.model['bin_max'].max()
        self.default_model = self.model[self.model['bin_max'] == self.max_bin].reset_index(drop=True)

    def fetch_model(self, name) -> pd.DataFrame:

        """
            Get the model from the database
        """

        try:

            db = Db('rca_db_prod')
            db.connect()

            table = sql.Identifier('models', name)
            string = sql.SQL('select * from {}').format(table)

            model = db.execute(string)
            db.disconnect()

            return model

        except Exception as e:
            print(str(e))
            raise Exception(f'Error getting model with name {name} are you sure it exists?')

    @staticmethod
    def build_db_model(df: pd.DataFrame, name: str) -> None:

        """
            Builds a model into our database. You must pass a dataframe with the following columns:

            df(count, mean, std, bin_min, bin_max)

            They must be exactly these columns, no more, no less.
        """

        # Make sure the types are clean
        df = df.astype({
            'count': 'int',
            'mean': 'float',
            'std': 'float',
            'bin_min': 'float',
            'bin_max': 'float'
        })

        # We save our models to our database, so first we need to make sure that we can get a valid connection
        db = Db('rca_db_prod')
        db.connect()

        # If a model with that name already exists, drop it
        table = sql.Identifier('models', name)
        string = sql.SQL("""

            -- Drop the existing version if it exists
            drop table if exists {};

            -- Create the table outline
            create table {} (
                id bigserial not null primary key,
                count int not null,
                mean float not null,
                std float not null,
                bin_min float not null,
                bin_max float not null
            );

        """).format(table, table)
        db.execute(string)
        db.big_insert(df, 'models.' + name)
        db.commit()

        db.disconnect()

        print(f'Model {name} created!')

    @staticmethod
    def delete(name: str) -> None:

        table = sql.Identifier('models', name)
        string = sql.SQL('drop table if exists {}').format(table)

        db = Db('rca_db_prod')
        db.connect()

        db.execute(string)
        db.commit()

        print(f'Model {name} deleted')

        db.disconnect()

    @staticmethod
    def create(df: pd.DataFrame, name: str) -> None:

        """

            This method creates a standardized model for dealing with this class.

            df:     This dataframe must be the result of the pandas .describe() method with all the indicies reset.
                    This dataframe must also have a column 'g' which is an IntervalIndex that describes how everything is binned (or 'grouped')
            name:   The name of the model that you're going to save. Use this to load an existing model later.
        
        """

        # If the min/max aren't already calculated
        if 'bin_min' not in df.columns and 'bin_max' not in df.columns:

            # Extract the bounds for each bin and add as a column
            df['bin_min'] = pd.IntervalIndex(df.g).left
            df['bin_max'] = pd.IntervalIndex(df.g).right

        df = df[['count', 'mean', 'std', 'bin_min', 'bin_max']].reset_index(drop=True)
        BinnedModel.build_db_model(df, name)

    def get_bin(self, x: int) -> Tuple[float, float]:

        if x >= self.max_bin:
            return self.default_model.loc[0, 'mean'], self.default_model.loc[0, 'std']

        mask = (
            (x >= self.model['bin_min']) & \
            (x < self.model['bin_max'])
        )

        m = self.model[mask].reset_index(drop=True)
        return m.loc[0, 'mean'], m.loc[0, 'std']

    def fit(self, x: int, y: float) -> Tuple[float, float, float]:

        """
        
            x: Your grouping value. For example, when looking at instagram data, we group by
                the number of instagram followers on any given day. It is the variable that you
                used for 'binning'
            y: Your 'gain' value. The value you are measuring as the dependent variable.

            returns: mean y in the x-bin, std y in the x-bin, z-score of y in x-bin
        
        """

        try:
            mean, std = self.get_bin(x)
        except Exception as e:
            print('x value: ')
            print(x)
            raise e
        
        z_score = (y - mean) / std

        return mean, std, z_score

    def visualize(self) -> None:

        m = self.model['mean']
        std = self.model['std']
        x = np.arange(0, len(std))

        fig, (ax1, ax2) = plt.subplots(1, 2)

        ax1.plot(x, m)
        ax1.set_title('Mean')

        ax2.plot(x, std)
        ax2.set_title('STD')

        fig.suptitle(f'Group means/std over {len(self.model)} bins')