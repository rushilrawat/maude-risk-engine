# FDA current-year refresh

This runbook downloads the 12 official base, add, and change archives linked by
the FDA MAUDE data-files page, reconciles them into four current tables, applies
quality checks, and publishes a new immutable snapshot.

## Configure storage

Copy `.env.example` to `.env` and set `MAUDE_DATA_ROOT` to an absolute path on a
volume with durable free space:

```bash
MAUDE_DATA_ROOT=/absolute/path/to/fda-maude-data
MAUDE_BATCH_ROWS=50000
```

The refresh retains content-addressed source ZIPs and immutable published
snapshots. During a changed refresh it also holds role-level and reconciled
Parquet staging data. Allow at least three times the total size of the 12
downloaded ZIPs in addition to space for snapshots already retained. Measure
the actual directory after an initial run before scheduling unattended runs.

## Run a refresh

```bash
uv run maude refresh-fda
```

The command accepts no source URL or snapshot ID. Discovery is restricted to
the current FDA catalog and its allowlisted MAUDE archive host.

Terminal statuses are:

- `PROMOTED`: a changed source set passed validation and is now current.
- `UNCHANGED`: all 12 source checksums match the current snapshot; published
  Parquet and `current.json` were not rewritten.
- `FAILED`: the prior current snapshot was preserved. The line includes the
  failed phase and the refresh-run evidence path.

Successful output includes the derived snapshot ID, catalog retrieval time,
and selected row counts by table family. Exit status is 0 for `PROMOTED` and
`UNCHANGED`, and 1 for `FAILED`.

## Evidence and published data

For each attempt, inspect:

- `MAUDE_DATA_ROOT/refresh-runs/<run-id>.json` for catalog, downloads, source
  hashes, reconciliation statistics, and failure evidence;
- `MAUDE_DATA_ROOT/raw/catalogs/` for preserved catalog responses;
- `MAUDE_DATA_ROOT/raw/sha256/` for admitted source ZIPs;
- `MAUDE_DATA_ROOT/manifests/<snapshot-id>.json` for snapshot quality and
  provenance;
- `MAUDE_DATA_ROOT/manifests/current.json` for the currently published
  snapshot; and
- `MAUDE_DATA_ROOT/silver/<snapshot-id>/` for the four current Parquet tables.

## Safe retries

The command is safe to rerun after a network or validation failure. It does not
move `current.json` until all source, reconciliation, quality, and promotion
checks pass. An identical source set reports `UNCHANGED`; a recoverable
interrupted promotion follows the existing snapshot recovery state.

Do not delete staging or manifests during an active or interrupted refresh.
Retain the failure manifest when investigating an operational error.

## Interpretation limitation

MAUDE is a passive-reporting system. Report counts and clusters are safety
signals for investigation, not incidence rates, causal findings, clinical risk
estimates, or proof that a device caused an event. Reporting practices,
duplicates, follow-up reports, missing fields, and market exposure can all
affect observed counts.
