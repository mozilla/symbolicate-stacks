# symbolicate-stacks

This program reads annotations for crash pings and reports, and symbolicates stack traces contained
within. It provides records compatible with the [siggen schema][] so that they may be fed directly
into siggen to create a signature.

The program downloads symbol files to perform symbolication if they are missing, and caches them for
future use. It runs as a server-client model, spawning a server as necessary. The server allows
efficient management of the cache, and keeps symbol files loaded so that unnecessary repeated
parsing and loading can be skipped.

[siggen schema]: https://github.com/mozilla-services/socorro-siggen#signature-generation-crash-data-schema
