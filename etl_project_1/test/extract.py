#!/usr/bin/python3
"""
Script to test extraction of data
"""
from src.etl_package_maiyo008.etl import Extract


df = Extract.load_parquet('yellow_tripdata_2025-01.parquet')
print(df.head())