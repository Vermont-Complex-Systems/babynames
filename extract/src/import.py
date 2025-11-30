import duckdb
from pyprojroot import here
import re
from pathlib import Path
import sys
import argparse


def load_US(conn):
    conn.execute("""
        CREATE TABLE babynames AS
        SELECT
            split_part(filename, '/', 3) AS countries,
            CAST(
                replace(
                    replace(split_part(filename, '/', 4), 'yob', ''),
                    '.txt', ''
                ) AS INTEGER
            ) AS year,
            name AS types,
            CAST(count AS INTEGER) AS counts,
            sex
        FROM read_csv_auto(
            'extract/input/united_states/yob*.txt',
            columns = {
                'name': 'TEXT',
                'sex': 'TEXT',
                'count': 'INT'
            },
            filename = TRUE
        );
    """)


def main(location):
    
    conn = duckdb.connect()
    conn.execute("ATTACH 'ducklake:metadata.ducklake' AS babylake (DATA_PATH '/users/j/s/jstonge1/data/babynames');")
    conn.execute("USE babylake;")
    
    if location == "United States":
        load_US(conn)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('location', default="United States")
    args = parser.parse_args()
    main(args.location)