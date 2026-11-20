## [Unreleased]

## [0.4.0] - 2026-11-20
- Allow configuration to be passed on the command line.
- Update the locked rustls version to address vulnerabilities.

## [0.3.0] - 2026-11-16
- Output the full error chain details on failure.
- Make `OOMAllocationSize` parsing more permissive, allowing a number or a string.

## [0.2.0] - 2026-10-02
- Reorganize example code into a subdirectory.
- Change to a standalone execution mode.
- Change caching to a purely fs-based approach to simplify concurrent access of multiple standalone
  programs.
- Forward report-only annotations that are relevant for siggen.

## [0.1.0] - 2026-09-02
- Initial release.

[Unreleased]: https://github.com/mozilla/symbolicate-stacks/compare/0.4.0...HEAD
[0.4.0]: https://github.com/mozilla/symbolicate-stacks/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/mozilla/symbolicate-stacks/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/mozilla/symbolicate-stacks/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/mozilla/symbolicate-stacks/tree/0.1.0
