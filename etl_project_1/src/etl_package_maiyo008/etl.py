#!/usr/bin/python3
"""
This is an ETL pipeline script that defines three classes(Extract, Transform, Load)
"""
import pandas as pd
import psycopg2


class Extract:
    """
    Class used to extract data from sources (PARQUET,)

    Methods:
        load_parquet(path): Loads a parquet file to a pandas data frame
    """
    @staticmethod
    def load_parquet(path:str):
        """
        Loads data from a parquet file to a pandas data frame

        Args:
            path(str): Path to the location of parquet file.

        Returns:
            df : A pandas data frame.

        Raises:
            FileNotFoundError: Error raised when wrong file path is submitted
            ImportError: Error raised when dependencies are not found
        """
        try:
            df = pd.read_parquet(path)
            return df
        except FileNotFoundError:
            print(f'File {path} not found')
        except ImportError:
            print('Required parquet engine not installed (pyarrow or fastparquet)')
        except Exception as e:
            print(f'Error in reading parquet file: {e}')

class Transform:
    """
    Class used to transform extracted data

    Methods:
        remove_duplicates(df): Remove duplicates in the dataframe
        remove_blanks(): Handle blank fields in the dataframe
    """
    @staticmethod
    def remove_duplicates(df):
        """
        Removes duplicate rows from the dataframe

        Args:
            df(DataFrame): Data frame

        Returns:
            df: A processed data frame without duplicates

        Raises:
        AttributeError: If a wrong object is passed instead of a data frame.
        """
        try:
            df = df.drop_duplicates(inplace=True)
            return df
        except AttributeError:
            print('Error: Passed parameter is not a pandas dataframe')
        except Exception as e:
            print(f'Error: {e}')

    @staticmethod
    def remove_blanks(df):
        """
        Removes rows with columns that are empty

        Args:
            df(DataFrame): Data frame

        Returns:
            df: A processed data frame without blank columns

        Raises:
            AttributeError: If a wrong object is passed instead of a data frame.

        """
        try:
            df = df.dropna(inplace=True)
            return df
        except AttributeError:
            print('Error: Passed parameter is not a pandas dataframe')
        except Exception as e:
            print(f'Error: {e}')

class Load:
    """
    Class used to load data to various destinations.

    Methods:
        connect_postgres(): Connect to postgres DB


    """
    @staticmethod
    def connect_postgres(database:str, host:str, user:str, password:str, port:int = 5432):
        """
        Connects to postgres database

        Args:
            database(str): database name
            host(str): host
            user(str): user
            password(str): password
            port(int): port (default=5432)
        
        Returns: A connection object

        Raises:
            ConnectionError: If passed args fails to create a connection to the database.

        """
        try:
            conn = psycopg2.connect(database = database, 
                        user = user, 
                        host= host,
                        password = password,
                        port = port)
            return conn
        except ConnectionError as e:
            print(f'Failed to connect to the database: {e}')
        except Exception as e:
            print(f'Error occured while connecting to database: {e}')