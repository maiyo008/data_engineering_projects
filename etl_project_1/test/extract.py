#!/usr/bin/python3
"""
Script to test extraction of data
"""
from src.etl_package_maiyo008.etl import Extract
from src.etl_package_maiyo008.etl import Transform
from src.etl_package_maiyo008.etl import Load
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT"))
TABLE_NAME = 'yellow_trip_data_2025'


df = Extract.load_parquet('yellow_tripdata_2025-01.parquet')
print(type(df))
df_no_dup = Transform.remove_duplicates(df)
print('Duplicates removed: ',type(df_no_dup))
df_transformed = Transform.remove_blanks(df_no_dup)
print('pd transformed',type(df_transformed))
engine = Load.connect_postgres(
    DB_NAME,
    DB_HOST,
    DB_USER,
    DB_PASSWORD,
    DB_PORT
)
Load.write_to_db(df_transformed, TABLE_NAME, engine)