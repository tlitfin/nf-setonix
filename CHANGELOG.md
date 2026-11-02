# nf-setonix changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.1.0 - 2026.11.02

Initial release:

- Added Setonix Slurm accounting via `sacct`.
- Added Slurm partition, GPU, node, state, and exit-code reporting.
- Calculated service units from Slurm billing TRES with Pawsey's documented
  partition charging formula as a fallback.
- Added CSV and JSON reports, cached-process accounting, and optional external
  report publishing.
