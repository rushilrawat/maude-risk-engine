# FDA historical MAUDE archive acquisition

This command acquires the historical core MAUDE source archives listed in the
official FDA data-files catalog. It does not parse or publish a historical
snapshot, and it does not change the current-year snapshot pointer.

## Scope

The allowlist contains 62 archives:

- Master and patient records through 2025.
- Device records through 1997, for 1998 and 1999, and annually from 2000 through
  2025.
- Narrative records through 1995 and annually from 1996 through 2025.

The catalog response is preserved with its URL, retrieval time, HTTP metadata,
and SHA-256. Each archive is downloaded from the FDA's approved archive host,
validated as a ZIP containing one supported source text file, checked against
its expected table family, and stored by SHA-256 under `raw/sha256/`. A run
manifest under `history-runs/` records the catalog and archive provenance.

This initial bootstrap excludes the separately packaged ASR and DEN datasets.
Device and patient problem-code association tables are not included in this
acquisition step; they require their own schema and historical reconciliation.

## Run on the external data volume

```bash
uv run maude download-fda-history \
  --data-root "/Volumes/SanDisk Ext/MAUDE/generated-data-validation"
```

Successful output reports the archive count, total compressed bytes, and run
manifest path. A failed run records the last completed archive and failure in
its manifest. Rerunning is safe: identical downloads resolve to the same
content-addressed raw objects, though the files are fetched again.

## Next step

Build a separate historical-corpus ingestion flow that parses the archives in
batches, preserves source-year provenance, reconciles business keys and
conflicts across years, validates cross-table report-key coverage, and publishes
an immutable historical snapshot. Keep the current-year refresh as a separate
input until annual rollover behavior is verified.

## FDA source and interpretation

The source is the [FDA MDR Data Files page](https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files).
FDA describes MAUDE as a passive-reporting system; report totals do not establish
incidence, comparative device risk, or causation.
