# symbolicate-stacks

This program reads annotations for crash pings and reports, and symbolicates stack traces contained
within. It provides records compatible with the [siggen schema][] so that they may be fed directly
into siggen to create a signature.

The program downloads symbol files to perform symbolication if they are missing, and caches them for
future use. It runs as a server-client model, spawning a server as necessary. The server allows
efficient management of the cache, and keeps symbol files loaded so that unnecessary repeated
parsing and loading can be skipped.

## Modes

The symbolicate-stacks program supports a few different modes of operation (keyed through
command-line subcommands).

### `server`

The `server` subcommand will start a symbolication server, using unix sockets for communication. The
socket and pid file of the server will be created under `$RUNTIME_DIR/symbolicate-stacks.sock` and
`$RUNTIME_DIR/symbolicate-stacks.pid`, where `RUNTIME_DIR` is `XDG_RUNTIME_DIR`, if set, otherwise
the temp directory.

Once running, the server will accept new socket connections and supports the following commands:

- `ping`: the server will respond with `pong` and close the connection.
- `close`: the server will shutdown.

Any other input is parsed as JSON crash annotations. Symbolication of the `StackTraces` annotation
then commences. Upon success, an `o` byte (for "output") is written to the connection, followed by
the JSON response. Upon failure, an `e` byte is written (for "error") and an error message follows.

#### Exit codes

Upon error, details will be logged.

- `0`: successful exit (indicating a clean shutdown)
- `1`: the server failed to start
- `2`: the server failed to start due to another server running or failure to acquire the PID file
  lock

#### Shutdown

If the server receives a `close` command, or receives a SIGINT, SIGTERM, or SIGHUP, it will exit the
main loop and shutdown.

### `client`

The `client` subcommand connects to a running server (started with the `server` command), reads
symbolication input from a file (`--input-file`, or stdin), sends it to the server, and prints the
result to `--output-file` or stdout (in case of success), or an error message to stderr.

#### Exit codes

- `0`: successful symbolication
- `1`: failed symbolication (printing the error from the server to stderr)
- `101`: fatal client error (printing panic details)

### `standalone`

The `standalone` subcommand runs one-shot symbolication without using a server. It accepts
`--config-file`, `--input-file`, and `--output-file` as the `server` and `client` subcommands do,
and behaves similarly.

#### Exit codes

- `0`: successful symbolication
- `1`: failed symbolication (printing the error to stderr)
- `101`: fatal client error (printing panic details)

## Logging

Logs are sent to stderr. The log level can be changed with `RUST_LOG`.

## Configuration

Symbolication is configured with a TOML file, through the `--config-file` command-line argument to
the `server` or `standalone` subcommands. If unspecified, `config.toml` is used. A config file must
be present.

See [example/config.toml](example/config.toml) for a commented file showing all configuration options.

## [run.py](example/run.py) Example

This python script will download crash pings from live tables, process, and upload the results.
Execute it directly for `uv` to automatically install dependencies. Note that you need proper
bigquery credentials configured to have access to the tables. It also requires the
`symbolicate-stacks` binary to be executable (in `PATH`), and as such the working directory also
needs a `config.toml` (the configuration is not explicitly specified).

This script is not meant for production use: it merely showcases a method of using
`symbolicate-stacks`.

[siggen schema]: https://github.com/mozilla-services/socorro-siggen#signature-generation-crash-data-schema
