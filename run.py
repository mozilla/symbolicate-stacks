#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "google-cloud-bigquery>=3.44.0",
#     "siggen==2.2.20241029",
# ]
# ///

# Download crash pings, symbolicate, and upload results.

from concurrent.futures import InterpreterPoolExecutor, as_completed
from concurrent.interpreters import create_queue, Queue
from contextlib import contextmanager
from google.cloud import bigquery
import json
import logging
import os
import subprocess
import typing
import uuid

PROJECT = 'moz-fx-data-shared-prod'
STAGE_PROJECT = 'moz-fx-data-shar-nonprod-efed'
DATASET = 'crash_ping_ingest_external'
SYMBOLICATE_STACKS = "symbolicate-stacks"

OUTPUT_TABLE_SCHEMA = [
    bigquery.SchemaField('document_id', 'STRING',
        description = "Identifier matching the document_id of a submitted ping.",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('submission_timestamp', 'TIMESTAMP',
        description = "The submission timestamp matching that of the submitted ping (used for partitioning).",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('crash_type', 'STRING',
        description = "The crash type, extracted from the ping stack trace data for convenience."
    ),
    bigquery.SchemaField('signature', 'STRING',
        description = "The crash signature generated from the symbolicated stack frames."
    ),
    bigquery.SchemaField('stack', 'RECORD',
        description = "Symbolicated stack frames for the crash ping.",
        mode = 'REPEATED',
        fields = [
            bigquery.SchemaField('offset', 'STRING',
                description = "The absolute offset of the frame.",
            ),
            bigquery.SchemaField('function', 'STRING',
                description = "The function symbol corresponding to the stack frame.",
            ),
            bigquery.SchemaField('function_offset', 'STRING',
                description = "The offset into the function, as a hex string.",
            ),
            bigquery.SchemaField('file', 'STRING',
                description = "The source file corresponding to the stack frame.",
            ),
            bigquery.SchemaField('line', 'INTEGER',
                description = "The source line corresponding to the stack frame.",
            ),
            bigquery.SchemaField('module', 'STRING',
                description = "The module corresponding to the stack frame.",
            ),
            bigquery.SchemaField('module_offset', 'STRING',
                description = "The offset into the module, as a hex string.",
            ),
            bigquery.SchemaField('omitted', 'INTEGER',
                description = "Whether frames were omitted. If this field is present, no other fields will be present.",
            ),
            bigquery.SchemaField('error', 'STRING',
                description = "An error message when generating the stack frame. If this field is present, no other fields will be present.",
            ),
        ],
    ),
]


class PingProcessor:
    _executor: InterpreterPoolExecutor

    @staticmethod
    def _pool_initializer():
        from siggen.generator import SignatureGenerator
        globals()["generator"] = SignatureGenerator()

    @staticmethod
    def _process_ping(annotations: str) -> str | tuple[str, str]:
        SYMBOLICATE_STACKS = "symbolicate-stacks"
        import json
        import subprocess

        symbolicated = subprocess.run([SYMBOLICATE_STACKS], text=True, input=annotations, capture_output=True)
        if symbolicated.returncode != 0:
            return f"symbolicate-stacks returned {symbolicated.returncode}: {symbolicated.stderr}"

        output = symbolicated.stdout

        try:
            sigdata = json.loads(output)
        except json.JSONDecodeError as e:
            return f"symbolicate-stacks returned invalid json ({e}): {symbolicated.stdout}"

        generate_result = globals()["generator"].generate(sigdata)
        return generate_result.signature, output

    def __init__(self, concurrent_count: typing.Optional[int] = None):
        # symbolicate-stacks will manage its concurrency well, so we want to
        # make sure it is has a good supply of work to do. It's better for work
        # to be queued in symbolicate-stacks than for latency to be introduced
        # by queueing in python.
        max_workers = concurrent_count or os.process_cpu_count() * 2
        self._executor = InterpreterPoolExecutor(max_workers = max_workers, initializer = PingProcessor._pool_initializer)

    def __enter__(self):
        self._executor.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.__exit__(exc_type, exc_val, exc_tb)

    def process_batch(self, rows: bigquery.table.RowIterator) -> typing.Iterable[tuple[str, str, str | tuple[str, str]]]:
        data = [dict(row) for row in rows]

        futures = {self._executor.submit(PingProcessor._process_ping, json.dumps(r)): r for r in data}
        
        for result_fut in as_completed(futures):
            row = futures[result_fut]
            result = result_fut.result()
            yield row["document_id"], row["submission_timestamp"], result


class BQ:
    _client: bigquery.Client
    _download_query: str
    _task_id: str

    def __init__(self, task_id: str, project: str):
        self._client = bigquery.Client(project)
        with open("download.sql", 'r') as f:
            self._download_query = f.read()
        self._task_id = task_id
        self.create_tables()

    def create_tables(self):
        tables = list(self._client.list_tables(DATASET))
        if not any(x.table_id == "live_ingest_log" for x in tables):
            table_ref = self._client.dataset(DATASET).table("live_ingest_log")
            table = bigquery.Table(table_ref, schema = [
                bigquery.SchemaField('time', 'TIMESTAMP',
                    description = "The time processing began.",
                    mode = 'REQUIRED',
                ),
                bigquery.SchemaField('task', 'STRING',
                    description = "The task processing the ping.",
                    mode = 'REQUIRED',
                ),
                bigquery.SchemaField('document_id', 'STRING',
                    description = "The document_id of the ping.",
                    mode = 'REQUIRED',
                ),
            ])
            self._client.create_table(table)
        if not any(x.table_id == "live_ingest_output" for x in tables):
            table_ref = self._client.dataset(DATASET).table("live_ingest_output")
            table = bigquery.Table(table_ref, schema = OUTPUT_TABLE_SCHEMA)
            table._properties["tableConstraints"] = {}
            table._properties["tableConstraints"]["primaryKey"] = {"columns": ["document_id"]}
            # We can't add a foreign key because the ids may pertain to ids in different glean tables.
            table.time_partitioning = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.DAY,
                field = "submission_timestamp",
                # 775 days, matching the crash tables
                expiration_ms = 1000 * 60 * 60 * 24 * 775,
                require_partition_filter = True,
            )

            self._client.create_table(table)

    def download(self) -> tuple[int, bigquery.table.RowIterator]:
        job_config = bigquery.QueryJobConfig(
            query_parameters = [
                bigquery.ScalarQueryParameter("task_id", bigquery.SqlParameterScalarTypes.STRING, self._task_id),
            ],
        )
        query_job = self._client.query(query, job_config = job_config)
        # Wait for job to complete (to ensure all child jobs are present)
        query_job.result()
        child_jobs = self._client.list_jobs(parent_job = query_job)
        select_data_job, count_job, *_ = (job for job in child_jobs if job.statement_type == "SELECT")
        return next(count_job.result(max_results=1))[0], select_data_job.result()

    def upload(self, data):
        self._client.load_table_from_json(
            data,
            self._client.dataset(DATASET).table("live_ingest_output")
        ).result()

    def clear_log(self):
        self._client.query_and_wait(
            f"DELETE FROM {DATASET}.live_ingest_log where task = @task_id;",
            job_config = bigquery.QueryJobConfig(
                query_parameters = [
                    bigquery.ScalarQueryParameter("task_id", bigquery.SqlParameterScalarTypes.STRING, self._task_id),
                ],
            ),
        )

def format_results(results: typing.Iterable[tuple[str, str, str, str]]) -> typing.Iterable[typing.Dict[str, typing.Any]]:
    for document_id, submission_timestamp, signature, sym_json in results:
        sym = json.loads(sym_json)
        threads = sym.get("threads", [])
        yield {
                'document_id': document_id,
                'submission_timestamp': submission_timestamp,
                'crash_type': sym.get("reason"),
                'signature': signature,
                'stack': threads[0].get("frames") if len(threads) > 0 else None
        }

def filter_errors(logger: logging.Logger, results: typing.Iterable[tuple[str, str, str | tuple[str, str]]]) -> typing.Iterable[tuple[str, str, str, str]]:
    for doc_id, timestamp, result in results:
        if type(result) == tuple:
            logger.debug(f"got result for {doc_id}")
            yield doc_id, timestamp, *result
        else:
            logger.error(f"error processing {doc_id}: {result}")


@contextmanager
def symbolicate_stacks_server():
    def runserver(arg: str):
        subprocess.run([SYMBOLICATE_STACKS, "--server", arg]).check_returncode()
    runserver("start")
    try:
        yield
    finally:
        runserver("stop")


if __name__ == "__main__":
    import argparse
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--loglevel', default='info', help="set the log level", choices=['critical','error','warning','info','debug'])
    parser.add_argument('--production', action='store_true', help="use production tables")

    args = parser.parse_args()

    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=args.loglevel.upper(), format="%(asctime)s %(levelname)s (%(name)s): %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    task_id = os.environ.get("TASK_ID") or str(uuid.uuid4())
    logger = logging.getLogger("main")

    with open("download.sql", 'r') as f:
        query = f.read()

    total_rows = 0
    success_rows = 0
    bq = BQ(task_id, PROJECT if args.production else STAGE_PROJECT)
    with symbolicate_stacks_server(), PingProcessor() as processor:
        while True:
            logger.info("querying ping data")
            avail_count, pings = bq.download()
            try:
                # Stop when we are getting fewer results to let a subsequent run do
                # the work.
                if avail_count < 100:
                    logger.info(f"not enough rows ({avail_count} available): exiting")
                    break

                logger.info(f"received {pings.total_rows} pings")

                total_rows += pings.total_rows

                results = processor.process_batch(pings)
                result_rows = list(format_results(filter_errors(logger, results)))

                success_rows += len(result_rows)

                logger.info(f"read {total_rows} ({total_rows-success_rows} failed)")

                bq.upload(result_rows)
            finally:
                bq.clear_log()
