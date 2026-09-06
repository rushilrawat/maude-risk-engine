# Local MAUDE archive ingestion

This runbook ingests canonical FDA MAUDE bulk archives from an operator-owned
directory. Local `*_UTF8.txt` files are hand conversions and are never
canonical inputs.

## Configure paths

Copy `.env.example` to `.env` and set `MAUDE_DATA_ROOT` to a separate,
ignored output directory. For example, `/Users/rushilrawat/MAUDE` may be the
source directory on one operator's machine, but that example is not a portable
or required location and should not also be used for generated outputs.

## Dry audit

Run the read-only audit before an ingestion:

```bash
uv run maude audit-local --data-root /Users/rushilrawat/MAUDE
```

It prints each selected archive, SHA-256 prefix, member, detected encoding,
header/table match, and any UTF-8 conversion comparison. Blocking findings
(bad archive/member, schema mismatch, missing or ambiguous required family,
or truncated conversion) return exit status 1. Warnings do not prevent a run.

## Ingest canonical archives

When the archive audit is clean, run:

```bash
uv run maude ingest-local local-2025-06 \
  --source-root /Users/rushilrawat/MAUDE
```

If a sibling `*_UTF8.txt` conversion is invalid but all canonical archives
pass the audit, use the narrow override below. It ignores only the hand
conversion; archive integrity, schema, table completeness, row conservation,
and referential checks still have to pass.

```bash
uv run maude ingest-local local-2025-06 \
  --source-root /Users/rushilrawat/MAUDE \
  --allow-archive-only
```

The command logs that it ignored the conversion. It writes the immutable run
manifest at `MAUDE_DATA_ROOT/manifests/local-2025-06.json`, the rendered report
at `MAUDE_DATA_ROOT/manifests/quality-report.md`, and promoted Silver Parquet
at `MAUDE_DATA_ROOT/silver/local-2025-06/`. `current.json` changes only after
promotion completes.

## Failures and retries

An audit-only refusal creates no manifest and can be rerun after its source
finding is addressed. For a terminal `FAILED` quality or pipeline manifest,
inspect its report, correct or replace the source input, and use a new snapshot
ID: the old terminal snapshot is immutable. Reuse the same snapshot ID only to
reconcile an interrupted promotion intent (for example, an `INCOMPLETE`
current-pointer publication); a different checksum for an existing snapshot ID
is deliberately rejected.

To confirm a failed run did not move the current pointer, compare the snapshot
ID before and after the attempt:

```bash
uv run python -c 'import json, sys; print(json.load(open(sys.argv[1]))["snapshot_id"])' \
  "$MAUDE_DATA_ROOT/manifests/current.json"
```

If the command failed before any snapshot had been promoted, `current.json`
may not exist; that also confirms no new current snapshot was published.
