# symbolicate-stacks

This program reads annotations for crash pings and reports, and symbolicates stack traces contained
within. It provides records compatible with the [siggen schema][] so that they may be fed directly
into siggen to create a signature.

The program downloads symbol files to perform symbolication if they are missing, and caches them for
future use. It runs as a server-client model, spawning a server as necessary. The server allows
efficient management of the cache, and keeps symbol files loaded so that unnecessary repeated
parsing and loading can be skipped.

## example/run.py
This python script will download crash pings from live tables, process, and upload the results.
Execute it directly for `uv` to automatically install dependencies. Note that you need proper
bigquery credentials configured to have access to the tables. It also requires the
`symbolicate-stacks` binary to be executable (in `PATH`), and as such the working directory also
needs a `config.toml` (config is not explicitly provided).

This script is not meant for production use: it merely showcases a method of using
`symbolicate-stacks`.

[siggen schema]: https://github.com/mozilla-services/socorro-siggen#signature-generation-crash-data-schema
