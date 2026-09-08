# FDA Current-Year Refresh Design

**Date:** 2026-09-07  
**Status:** Proposed  
**Scope:** Phase 2A — manual refresh of the four current-year FDA MAUDE table families

## 1. Outcome and success criteria

Add one production-style command, `maude refresh-fda`, that discovers the official current-year MAUDE archives from the FDA MDR Data Files page, downloads immutable copies, reconciles base/add/change records, runs the existing quality gates, and atomically publishes a new current snapshot.

The feature is complete when:

1. Catalog fixtures select exactly the 12 allowed current-year archives: base, add, and change for master, device, narrative, and patient.
2. A partial, corrupt, oversized, wrong-host, wrong-member, or schema-invalid download is never admitted to immutable storage.
3. Repeating a refresh whose 12 SHA-256 checksums are unchanged returns the existing result without rewriting snapshot data or `current.json`.
4. Fixture rows reconcile deterministically by the existing business keys, with change rows replacing matching base/add rows and new child keys being inserted.
5. Any discovery, download, parsing, reconciliation, quality, or promotion failure leaves the previously published `current.json` unchanged and records exact failure evidence.
6. A real invocation either promotes the current FDA data or reports the unchanged blocking evidence needed to diagnose why it did not.

## 2. Source authority and observed contract

The sole catalog for this phase is the [FDA MDR Data Files page](https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files#download). FDA describes the ZIP data as updated monthly, recommends downloading all record types for the period of interest, and states that the files join through `MDR_REPORT_KEY`.

The current page exposes these required archives:

| Family | Base | Add | Change |
|---|---|---|---|
| master | `mdrfoi.zip` | `mdrfoiadd.zip` | `mdrfoichange.zip` |
| device | `device.zip` | `deviceadd.zip` | `devicechange.zip` |
| narrative | `foitext.zip` | `foitextadd.zip` | `foitextchange.zip` |
| patient | `patient.zip` | `patientadd.zip` | `patientchange.zip` |

The catalog parser extracts URLs from the live page; it does not construct or hard-code yearly URLs. The filenames above are an allowlist and classification contract, not download locations. Historical, problem-code, ASR, and DEN links are out of scope.

FDA describes base files as current-year records, add files as new records for the current month, and change files as updates. Device and narrative change files may also contain additional child records for existing master reports. That makes business-key upsert the common reconciliation rule for all four families.

## 3. Scope boundaries

### Included

- One operator-run command: `maude refresh-fda`.
- Live discovery from the official catalog page.
- Streaming download and immutable checksum-addressed raw storage.
- All 12 current-year base/add/change archives.
- Deterministic reconciliation into one current table per family.
- Existing parsing, normalization, quality thresholds, failure evidence, and atomic current-pointer behavior.
- Source and row-level provenance sufficient to reproduce every selected row.

### Deferred

- A scheduler or background worker.
- Historical bootstrap or annual rollover.
- openFDA or any non-bulk enrichment.
- Device/patient problem-code tables.
- TF-IDF, clustering, LangChain, LangGraph, embeddings, retrieval, or an LLM.
- A public API, dashboard, Tableau extract, or deployment configuration.

The manual command establishes the reliable data boundary those later layers require.

## 4. Assumptions and invariants

- Only `https://www.fda.gov/.../mdr-data-files` may supply the catalog.
- Archive URLs and every redirect target must use HTTPS and the exact host `www.accessdata.fda.gov`.
- The catalog must yield exactly one URL for every `(family, role)` pair. Missing or duplicate matches fail closed.
- A ZIP must pass the existing CRC/member-safety checks, contain exactly one non-directory `.txt` member, and match the expected family schema.
- A role archive must be classified from its allowlisted link filename. The member filename is separately checked for the same family and role, so a valid ZIP served under the wrong link cannot pass.
- Base, add, and change archives are a publication set observed at one catalog retrieval time. FDA does not publish an atomic set identifier; the manifest therefore records the exact 12 content hashes and the catalog retrieval evidence.
- Absence from add/change never means delete.
- The source provides no row-level change timestamp. The system must not invent event-effective `valid_from`/`valid_to` values.
- Existing immutable published snapshots retain earlier observable row versions. Within a snapshot, raw role archives and selected-row provenance retain the inputs needed to reconstruct superseded base/add rows.

That last invariant deliberately represents history by immutable observed snapshots, rather than claiming FDA supplied temporal semantics it does not provide.

## 5. Data contracts

Add a `SourceRole` enum with `base`, `add`, and `change`.

Add strict refresh models for:

- **Catalog evidence:** catalog URL, retrieval timestamp, final URL, HTTP ETag/Last-Modified when supplied, page SHA-256, and immutable HTML path.
- **Discovered artifact:** table family, source role, allowlisted filename, and resolved FDA URL.
- **Downloaded artifact:** discovered fields plus download start/completion timestamps, final URL, HTTP ETag/Last-Modified when supplied, byte size, SHA-256, raw path, archive member, encoding, and detected header.
- **Reconciliation statistics:** inserted, updated, unchanged, and superseded row counts by family and role.
- **Refresh-run manifest:** run ID, optional refresh fingerprint once all 12 hashes exist, catalog evidence, downloaded artifacts collected so far, reconciliation statistics, run status, terminal failure evidence, and the promoted snapshot ID when successful.

Extend row provenance with `_source_role`. The normalized selected row continues to carry `_source_line_number`, `_source_snapshot_sha256`, `_source_filename`, `dataset_snapshot_id`, and `record_content_hash`.

`TableResult` must represent all contributing sources for a reconciled table. The minimal contract change is to add a `sources` sequence while retaining the existing singular `source` as the primary/base source for current local-ingestion compatibility. Refresh results populate `sources` in base/add/change order; local ingestion populates it with its one source. `SnapshotResult.inputs` remains the authoritative run-wide list.

No new public query schema is introduced in this phase.

## 6. Storage layout

Use the existing configured `MAUDE_DATA_ROOT` and add:

```text
raw/
  catalogs/<page-sha256>.html
  sha256/<first-two>/<archive-sha256>.zip
refresh-runs/
  <run-id>.json
staging/
  <snapshot-id>/...
silver/
  <snapshot-id>/{master,device,narrative,patient}.parquet
manifests/
  <snapshot-id>.json
  current.json
```

Raw filenames are content hashes, so a changed file at a stable FDA URL becomes a new object and an unchanged file is reused. Temporary downloads are created beside the raw target, flushed and `fsync`ed, inspected, and atomically renamed only after all validation succeeds. Failed temporary files are removed; an already published raw object is never overwritten.

The refresh-run manifest is separate from the existing snapshot manifest because it records catalog and network state, including failures that occur before a content fingerprint can exist. It is atomically written using the current manifest helper's durable JSON-write primitive.

## 7. Components and flow

### 7.1 Catalog discovery

`maude.ingestion.fda_catalog` uses the standard-library HTML parser to inspect anchor `href` values. It resolves relative links against the catalog URL, normalizes only URL syntax, and selects anchors whose basename exactly matches the 12-file allowlist.

Discovery rejects:

- non-HTTPS catalog or archive URLs;
- user-info, fragments, unexpected ports, or non-allowlisted hosts;
- missing or duplicate `(family, role)` entries; and
- allowlisted-looking filenames outside the catalog’s MAUDE download section.

The result is deterministically ordered by `TableKind`, then `SourceRole`.

### 7.2 Streaming download

`maude.ingestion.download` uses the Python standard library; no HTTP dependency is added. A small opener protocol allows deterministic fake responses in tests.

For each artifact it:

1. Opens the discovered URL with an explicit network timeout and a fixed user agent.
2. Validates the final URL after redirects.
3. Streams bounded blocks into a unique temporary file while computing SHA-256 and counting bytes.
4. Rejects a body larger than a fixed 2 GiB safety ceiling and rejects a mismatching `Content-Length` when that header is present.
5. Flushes and syncs the completed temporary file.
6. Runs existing archive integrity, encoding, header, family, and member-role checks.
7. Atomically admits the file at its checksum-addressed raw path, or reuses an identical existing object.

Resumable downloads, parallel downloads, retries, and conditional GETs are deferred. A failed run can safely be invoked again because validated raw objects are reused by checksum.

### 7.3 Parse and reconcile

Each of the 12 validated raw archives is parsed and normalized with the existing streaming parser and schema definitions. Parsing adds the known source role to every accepted row.

For each table family, DuckDB builds the selected current table in one bounded-memory query:

- partition by the existing `TableSpec.business_key`;
- order candidates by role priority `change > add > base`;
- require uniqueness of a business key within each individual role archive; and
- retain the highest-priority row, including its original provenance columns.

Across roles:

- a new key inserts a row;
- the same key and same `record_content_hash` is unchanged;
- the same key and different hash is updated by the higher-priority role; and
- no record is deleted because it is absent from a later role.

Within-role duplicate business keys are blocking ambiguity, not “last line wins.” Reconciliation records inserted, updated, unchanged, and superseded counts by family and role in the refresh-run manifest.

The current business keys remain:

- master: `MDR_REPORT_KEY`;
- device: `MDR_REPORT_KEY`, `DEVICE_SEQUENCE_NO`;
- patient: `MDR_REPORT_KEY`, `PATIENT_SEQUENCE_NUMBER`; and
- narrative: `MDR_TEXT_KEY`.

### 7.4 Quality and publication

The reconciled four-table snapshot enters the existing quality and promotion path. Required tables, field counts, reject fractions, business-key uniqueness, child-to-master orphan fractions, date parsing, and prior-current row-count drift retain their current thresholds.

No code path writes `manifests/current.json` until:

- all 12 artifacts are discovered, downloaded, and validated;
- all role files parse;
- all four tables reconcile;
- all blocking quality checks pass; and
- promoted Parquet artifacts and their manifest are durable.

On failure, the refresh and snapshot manifests contain the failed phase, family/role when known, exception type, message, and all source identities collected so far. The existing current pointer remains byte-for-byte unchanged.

## 8. Identity and idempotency

After all downloads validate, compute the refresh fingerprint as SHA-256 over a canonical JSON array of `(family, role, artifact_sha256)` in deterministic order. The snapshot ID is `fda-current-<first-16-fingerprint-chars>`.

If `current.json` already points to a snapshot with that fingerprint, `maude refresh-fda` reports `UNCHANGED` and returns success without parsing, rewriting Parquet, or updating `current.json`. The invocation may still write its refresh-run manifest so the attempted network observation is auditable.

If an immutable snapshot with that fingerprint exists but is not current, the source publication has returned to previously observed bytes. The system validates the existing artifacts, reruns the existing quality checks against the current snapshot, and only then atomically republishes that snapshot. It never reports this case as unchanged.

If an earlier snapshot run with that fingerprint was interrupted or failed, normal recovery rules apply. A fingerprint collision with different source identities is a blocking conflict.

HTTP ETag, Last-Modified, size, and URL are provenance and diagnostics; content SHA-256 is the identity used for idempotency.

## 9. CLI behavior

`maude refresh-fda` takes no source URL and no snapshot ID. It uses `MAUDE_DATA_ROOT`, the fixed official catalog, and the derived fingerprint-based snapshot ID.

Terminal output is one concise status line followed by family counts:

- `PROMOTED <snapshot-id> ...` with the catalog retrieval time;
- `UNCHANGED <snapshot-id> ...`; or
- `FAILED <phase> ...` with the refresh-manifest path.

Promoted and unchanged outcomes exit `0`. Any incomplete or failed outcome exits `1`. Detailed evidence remains in manifests and the existing Markdown quality report rather than being dumped to the terminal.

## 10. Verification strategy

Implementation follows test-first, incremental checkpoints:

1. **Catalog contract** — fixtures prove exact 12-file selection, deterministic ordering, relative-link resolution, section scoping, and fail-closed host/missing/duplicate behavior.
2. **Downloader safety** — fake streaming responses prove checksum storage, metadata capture, length/size enforcement, redirect validation, corrupt ZIP rejection, temporary cleanup, and raw-object reuse.
3. **Role parsing** — fixtures prove `_source_role` and the existing source provenance survive normalization.
4. **Reconciliation** — fixtures prove inserts, identical duplicates, changed replacements, added child keys, within-role duplicate rejection, deterministic output, and no deletion by absence for every business-key shape.
5. **Refresh orchestration** — integration tests prove fingerprint idempotency, recoverable failure evidence, and no mutation of a pre-existing `current.json` on every failure phase.
6. **CLI** — tests prove promoted/unchanged/failed output and exit codes.
7. **Regression** — `make verify` remains green.
8. **Official-source smoke test** — run the catalog discovery against the live FDA page, then run one real `maude refresh-fda`; promotion is preferred, but an evidence-complete quality failure is an acceptable diagnostic outcome and must leave the prior snapshot live.

The live smoke test is not part of the deterministic unit suite.

## 11. Known limitations after this phase

- Freshness is operator-triggered, not scheduled.
- Only the current-year publication set is represented; it is not yet a complete historical research corpus.
- FDA does not provide an atomic timestamp across the 12 links, so a manifest proves exactly what was observed, not that FDA generated every file simultaneously.
- MAUDE is a passive-reporting dataset. Counts are reporting signals, not incidence, causal attribution, or clinical risk estimates.
- If FDA moves MAUDE delivery to AEMS or changes link/schema contracts, discovery fails closed and the adapter must be updated deliberately.
