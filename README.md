# symbolicate-stacks

This program reads annotations for crash pings and reports, and symbolicates stack traces contained
within. It provides records compatible with the [siggen schema][] so that they may be fed directly
into siggen to create a signature.

The program downloads symbol files to perform symbolication if they are missing, and caches them for
future use.

## Usage

`symbolicate-stacks` accepts the following arguments:

### `--config-file <FILE>`

Specify the config file to use. Defaults to `config.toml`.

### `--input-file <FILE>`

Specify the input file from which to read. Defaults to stdin.

### `--output-file <FILE>`

Specify the output file to which to write. Defaults to stdout.

### `--clean-cache [<BYTES>]`

Clean the cache, evicting entries to reduce usage to the given maximum size. If `<BYTES>` is
omitted, the size is read from the config file. On the command-line, the size may be suffixed by
`k`, `M`, `G`, or `T` to multiply the value.

## Exit codes

- `0`: successful symbolication or cache cleanup
- `1`: failed symbolication (printing the error to stderr)
- `101`: fatal client error (printing panic details)

## Input

The input is a JSON document containing crash annotations (in the same format as is used for
annotations stored alongside crashes in Firefox). Additionally, the input annotation keys may
arbitrarily be either snake_case or CamelCase (the former is a bit more conventional to use in,
e.g., SQL queries).

## Output

The output is a JSON document compatible with the [siggen schema][]. It contains as much information
as possible from the input annotations (i.e., the program supports reading the annotations necessary
to populate the fields for siggen).

## Logging

Logs are sent to stderr. The log level can be changed by setting the `RUST_LOG` environment variable
to choose the log level and filtering (see the [env_logger docs][]).

## Configuration

Symbolication is configured with a TOML file, through the `--config-file` command-line argument. If
unspecified, `config.toml` is used. A config file must be present.

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
[env_logger docs]: https://docs.rs/env_logger/latest/env_logger/index.html#enabling-logging
