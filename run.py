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

from concurrent.futures import InterpreterPoolExecutor
from concurrent.interpreters import create_queue, Queue
from google.cloud import bigquery
import json
import logging
import os
from siggen.generator import SignatureGenerator
import subprocess
import typing
import uuid

PROJECT = 'moz-fx-data-shared-prod'
STAGE_PROJECT = 'moz-fx-data-shar-nonprod-efed'
DATASET = 'crash_ping_ingest_external'

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
    _work: Queue
    _results: Queue

    @staticmethod
    def _process(num: int, q: Queue, results: Queue):
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(f"ping processor {num}")
        logger.debug("started processor")
        generator = SignatureGenerator()
        while True:
            doc_id, timestamp, annotations = typing.cast(tuple, q.get())
            logging.debug("received data to process")
            result = None
            try:
                logging.debug("launching symbolicate-stacks")
                symbolicated = subprocess.run(["./target/release/symbolicate-stacks"], text=True, input=annotations, capture_output=True)
                if symbolicated.returncode != 0:
                    logger.error(f"symbolicate-stacks returned {symbolicated.returncode}: {symbolicated.stderr}")
                    continue

                output = symbolicated.stdout

                try:
                    sigdata = json.loads(output)
                except json.JSONDecodeError as e:
                    logger.error(f"symbolicate-stacks returned invalid json {e}: {symbolicated.stdout}")
                    continue

                generate_result = generator.generate(sigdata)
                result = (doc_id, timestamp, generate_result.get('signature'), output)
            finally:
                results.put(result)

    def __init__(self, concurrent_count: int = 32):
        self._executor = InterpreterPoolExecutor(max_workers = concurrent_count)
        self._work = create_queue()
        self._results = create_queue()

        for i in range(concurrent_count):
            self._executor.submit(PingProcessor._process, i+1, self._work, self._results)

    def push(self, *args):
        self._work.put(args)

    def wait(self, count: int) -> list[typing.Optional[tuple[str, str, str, str]]]:
        results = []
        while len(results) < count:
            results.append(typing.cast(tuple, self._results.get()))
        return results

    def process_batch(self, rows: bigquery.table.RowIterator) -> list[tuple[str, str, str, str]]:
        # Read all data up-front to get it in memory and close the iterator's connection.
        total = rows.total_rows
        for row in rows:
            document_id = row.get("document_id")
            timestamp = row.get("submission_timestamp")
            self.push(document_id, timestamp, json.dumps(dict(row)))
        return list(filter(None, self.wait(total)))

class BQ:
    _client: bigquery.Client
    _download_query: str
    _task_id: str

    def __init__(self, task_id: str):
        self._client = bigquery.Client(STAGE_PROJECT)
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

    def download(self) -> bigquery.table.RowIterator:
        job_config = bigquery.QueryJobConfig(
            query_parameters = [
                bigquery.ScalarQueryParameter("task_id", bigquery.SqlParameterScalarTypes.STRING, self._task_id),
            ],
        )
        query_job = self._client.query(query, job_config = job_config)
        # Wait for job to complete (to ensure all child jobs are present)
        query_job.result()
        child_jobs = self._client.list_jobs(parent_job = query_job)
        select_job = next(job for job in child_jobs if job.statement_type == "SELECT")
        return select_job.result()

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

def format_results(results: list[tuple[str, str, str, str]]) -> typing.Iterable[typing.Dict[str, typing.Any]]:
    for document_id, submission_timestamp, signature, sym_json in results:
        sym = json.loads(sym_json)
        threads = sym["threads"]
        yield {
                'document_id': document_id,
                'submission_timestamp': submission_timestamp,
                'crash_type': sym.get("reason"),
                'signature': signature,
                'stack': threads[0]["frames"] if len(threads) > 0 else None
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    task_id = os.environ.get("TASK_ID") or str(uuid.uuid4())
    logger = logging.getLogger("run.py main")

    with open("download.sql", 'r') as f:
        query = f.read()

    total_rows = 0
    success_rows = 0
    processor = PingProcessor()
    bq = BQ(task_id)
    while True:
        logger.debug("querying ping data")
        pings = bq.download()
        try:
            # Stop when we are getting fewer results to let a subsequent run do
            # the work.
            if pings.total_rows < 100:
                logger.info(f"not enough rows (received {pings.total_rows}): exiting")
                break

            logger.debug(f"received {pings.total_rows} pings")

            total_rows += pings.total_rows
            results = processor.process_batch(pings)
            success_rows += len(results)

            logger.info(f"read {total_rows} ({total_rows-success_rows} failed)")

            bq.upload(format_results(results))
        finally:
            bq.clear_log()
