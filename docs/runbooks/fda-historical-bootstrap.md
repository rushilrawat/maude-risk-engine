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
its manifest. Rerunning with the same preserved FDA catalog resumes previously
validated downloads after checking their stored checksums, then fetches only
missing or invalid archives. A changed catalog triggers a fresh acquisition.

## Parse and publish the historical snapshot

Pass the successful acquisition manifest to the historical ingestion command:

```bash
uv run maude ingest-fda-history \
  "/Volumes/SanDisk Ext/MAUDE/generated-data-validation/history-runs/<run-id>.json" \
  --data-root "/Volumes/SanDisk Ext/MAUDE/generated-data-validation"
```

The command parses each source in bounded batches, preserves archive and line
provenance, normalizes each annual part, reconciles duplicate business keys and
quarantines conflicting rows. It applies the existing row-conservation,
rejection-rate, uniqueness, and cross-table report-key checks before publishing
an immutable `fda-history-<fingerprint>` snapshot under `silver/`. DuckDB spill
files and staging output use the configured data volume. It does not update
`manifests/current.json`; current-year data remain a separate snapshot until
annual rollover behavior is verified.

If ingestion fails, inspect `manifests/fda-history-<fingerprint>.json` and its
quality findings. No partial historical snapshot is promoted. Rerunning uses the
preserved raw archives and starts the failed snapshot build again.

After promotion, the existing corpus command can build the narrative dataset:

```bash
uv run maude build-corpus fda-history-<fingerprint> \
  --data-root "/Volumes/SanDisk Ext/MAUDE/generated-data-validation"
```

Device and patient problem-code association tables remain a separate follow-up;
their schemas and historical reconciliation are not included in this core
four-table snapshot.

## FDA source and interpretation

The source is the [FDA MDR Data Files page](https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files).
FDA describes MAUDE as a passive-reporting system; report totals do not establish
incidence, comparative device risk, or causation.
