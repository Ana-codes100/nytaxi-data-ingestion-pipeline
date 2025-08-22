#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    file_name = url.split("/")[-1]
    os.system(f"wget {url} -O {file_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if file_name.endswith('.parquet'):
        df = pd.read_parquet(file_name)
    elif file_name.endswith('.csv.gz'):
        df = pd.read_csv(file_name, compression='gzip')
    else:
        df = pd.read_csv(file_name)

    datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    chunk_size = 100000
    total_rows = len(df)
    for i in range(0, total_rows, chunk_size):
        t_start = time()

        df.iloc[i:i+chunk_size].to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f"Inserted rows {i} to {min(i+chunk_size, total_rows)}, took {t_end - t_start:.3f} seconds")

    print("Finished ingesting data into the postgres database")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    parser.add_argument('--user', required=True, help='Postgres username')
    parser.add_argument('--password', required=True, help='Postgres password')
    parser.add_argument('--host', required=True, help='Postgres host')
    parser.add_argument('--port', required=True, help='Postgres port')
    parser.add_argument('--db', required=True, help='Postgres database name')
    parser.add_argument('--table_name', required=True, help='Name of the table to write to')
    parser.add_argument('--url', required=True, help='URL of the file to download')

    args = parser.parse_args()

    main(args)
