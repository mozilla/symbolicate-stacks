#!/usr/bin/env python3
# Download crash pings, symbolicate, and upload results.

from google.cloud import bigquery
import typing

PROJECT = 'moz-fx-data-shared-prod'

# Returns a batch of ping rows
def download_batch(query: str) -> bigquery.table.RowIterator:
    client = bigquery.Client(PROJECT)
    return client.query_and_wait(query)

def write_rows(rows: bigquery.table.RowIterator, output: typing.IO[str]):
    for row in rows:
        json.dump(dict(row), output, separators=(',',':'))
        output.write("\n")

def process_batch(rows: bigquery.table.RowIterator):
    # Read all data up-front to get it in memory and close the connection.
    data = [dict(row) for row in rows]
    for row in data:
        pass

if __name__ == "__main__":
    import json

    with open("download.sql", 'r') as f:
        query = f.read()

    pings = download_batch(query)

    write_rows(config, config_output)
    write_rows(pings, pings_output)
