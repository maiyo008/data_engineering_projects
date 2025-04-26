#!/usr/bin/python3
"""
This is an ETL pipeline script that defines three classes(Extract, Transform, Load)
"""
import pandas as pd


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

