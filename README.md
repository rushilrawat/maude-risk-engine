<div align="center">

# 🏥 MAUDE Risk Engine

**Reproducible reporting-signal analysis for FDA medical-device adverse-event data.**

*Official records in. Auditable failure-pattern evidence out. No clinical risk claims.*

[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/analytics-DuckDB-FFF000.svg?logo=duckdb&logoColor=black)](https://duckdb.org/)
[![PyArrow](https://img.shields.io/badge/storage-Parquet-4B8BBE.svg)](https://arrow.apache.org/)
[![status](https://img.shields.io/badge/status-foundation%20in%20progress-orange.svg)](#-development-roadmap)

</div>

---

## 🎯 The purpose

MAUDE Risk Engine turns the FDA's public Manufacturer and User Facility Device
Experience data into a versioned research corpus for investigating recurring and
emerging medical-device failure patterns.

The finished platform will combine transparent statistical methods, narrative-text
clustering, evidence retrieval, and a persistent human-review workflow. The
analytical system remains useful without an LLM: deterministic data processing and
scikit-learn models are authoritative, while LangChain and LangGraph coordinate
research tasks where durable state, branching, and review are genuinely useful.

This is a **reporting-signal exploration and triage tool**, not a clinical risk
calculator. It does not estimate incidence, determine causality, compare device
safety, diagnose patients, or replace FDA review.

## ✨ Product principles

- 🏛️ **FDA first:** the initial system uses official FDA sources only.
- 🔗 **Evidence before interpretation:** every output traces back to immutable source
  files, snapshot manifests, and source rows.
- 📐 **Statistics stay authoritative:** no language model calculates or changes a
  signal score.
- 🔁 **Reproducible by design:** inputs, schemas, features, models, and outputs are
  versioned independently.
- 👤 **Humans retain judgment:** severe, ambiguous, or low-confidence cases remain in
  review until a person records a disposition.
- ⚠️ **Claims stay narrow:** the interface says “reporting pattern” and “triage
  priority,” never “dangerous device” or “proven harm.”

## 🏗️ How it fits together

```text
                 Official FDA MAUDE files
                            │
                            ▼
            catalog discovery + immutable hashes
                            │
                            ▼
              Bronze: normalized source records
                            │
                  quality gates + rejects
                            │
                            ▼
       Silver: master · device · patient · narrative
                            │
               base/add/change reconciliation
                            │
             ┌──────────────┴──────────────┐
             ▼                             ▼
   descriptive signal facts      TF-IDF + clustering
             │                             │
             └──────────────┬──────────────┘
                            ▼
                 cited evidence retrieval
                            │
                            ▼
                LangGraph research triage
                 /          │           \
            routine    investigate    quarantine
                            │
                       human review
                            │
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
          public API     web app       Tableau/research
```

LangGraph is planned for case orchestration, not ETL scheduling. LangChain will
provide typed retrieval and analysis tools. An optional LLM may eventually draft a
cited investigation brief from an already selected evidence packet; it cannot
select evidence silently, infer causality, or finalize a case.

## ✅ What works today

- Discovers the expected current FDA archive families from the official catalog.
- Downloads allowed FDA URLs into content-addressed storage and records SHA-256
  evidence.
- Audits local canonical ZIP archives without trusting hand-converted UTF-8 files.
- Streams large pipe-delimited files into typed, versioned Parquet rather than
  loading the full corpus into memory.
- Reconciles FDA base, add, and change roles with deterministic precedence.
- Collapses content-identical duplicates and quarantines conflicting duplicate keys
  with their original evidence rows.
- Applies blocking quality gates before atomically publishing a new `current`
  snapshot.
- Preserves manifests, rejects, reconciliation counts, and recovery information.
- Supports safe retries and keeps the last validated snapshot live after failure.

## 🚀 Quickstart

**Prerequisites:** Python 3.12 and [`uv`](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/rushilrawat/maude-risk-engine.git
cd maude-risk-engine
uv sync --all-extras
cp .env.example .env
make verify
```

Audit operator-owned local FDA archives:

```bash
uv run maude audit-local --data-root /path/to/fda-archives
```

Ingest an audited local snapshot:

```bash
uv run maude ingest-local local-2025-06 \
  --source-root /path/to/fda-archives
```

Discover, download, reconcile, validate, and publish the latest official current
files:

```bash
uv run maude refresh-fda
```

Operational details live in the
[`local ingestion`](docs/runbooks/local-ingestion.md) and
[`current FDA refresh`](docs/runbooks/fda-current-refresh.md) runbooks.

## 🔬 Scientific and claim boundaries

FDA MAUDE is a passive-reporting system. Reports can be incomplete, duplicated,
delayed, unverified, or influenced by reporting and publicity patterns. Therefore:

- report counts are not numbers of harmed patients;
- changes in report volume are not incidence estimates;
- a cluster is not proof of a shared root cause;
- severity means reported severity, not inferred clinical severity;
- recall information, when added, will remain contextual evidence rather than proof
  that a cluster caused a recall; and
- every public result must display its dataset snapshot, feature/model version,
  evidence links, and limitations.

## 🧪 Verification strategy

```bash
make verify
```

The verification suite runs formatting, linting, strict type checking, and tests.
Coverage includes schema signatures, encoding detection, row conservation,
referential checks, manifest immutability, duplicate reconciliation, publication
rollback, and CLI behavior. Network-free fixtures keep routine tests reproducible.

Future analytical validation will add temporal holdouts, bootstrap and seed
stability, cluster coherence review, synthetic emerging-pattern tests, known-case
regressions, snapshot-to-snapshot drift reports, and human reviewer agreement.

## 🗺️ Development roadmap

Effort describes technical complexity, not calendar time. Checked items are present
in the current implementation; unchecked items describe the complete planned
system, not promises that they already exist.

### 1. [x] FDA source foundation — **High**

- Discover and allowlist official current-year MAUDE archives.
- Preserve source URLs, timestamps, HTTP metadata, checksums, archive members, and
  parser/schema versions.
- Support immutable content-addressed downloads and idempotent reruns.

### 2. [x] Text and row cleanup — **High**

- Detect source encoding and normalize Unicode, whitespace, control characters,
  delimiters, dates, and missing values without losing raw evidence.
- Reject malformed rows with source location and machine-readable reasons.
- Collapse exact duplicates and quarantine conflicting business keys.

### 3. [x] Canonical MAUDE data model — **Medium**

- Normalize master, device, patient, and narrative tables into typed Parquet.
- Retain stable business keys, raw strings where needed, snapshot IDs, and row-level
  provenance.
- Keep bounded Pandas use for exploration and modeling rather than bulk loading.

### 4. [x] Reconciliation and publication — **High**

- Apply deterministic base/add/change precedence.
- Record inserted, superseded, unchanged, collapsed, and quarantined counts.
- Publish only quality-gated snapshots with atomic pointer updates and rollback-safe
  recovery.

### 5. [ ] Complete production corpus — **High**

- Finish and document a clean end-to-end live refresh across every required table.
- Add historical annual files, device/patient problem codes, and controlled annual
  rollover reconciliation.
- Add FDA device classification and recall context through separate, versioned
  enrichment pipelines.
- Prepare a source adapter boundary for the FDA's evolving adverse-event systems.

### 6. [ ] Narrative and feature engineering — **High**

- Construct one evidence-linked document per report while preserving narrative-row
  boundaries and text types.
- Mask repeated regulatory boilerplate only in modeling representations.
- Track exact and near-duplicate narratives so repeated submissions cannot dominate
  evaluation unnoticed.
- Build word and character TF-IDF plus frequency, reported-severity, novelty,
  confidence, and report-volume-change features.

### 7. [ ] Clustering and statistical evaluation — **XHigh**

- Benchmark sparse and reduced TF-IDF representations using MiniBatchKMeans as the
  scalable baseline.
- Train within sufficiently populated product-code or device-family cohorts, with a
  documented fallback for sparse cohorts.
- Select configurations using multiple seeds, bootstrap stability, coherence,
  reviewer usefulness, and temporal holdouts—not silhouette score alone.
- Backtest synthetic emerging patterns, measure alert burden and false positives,
  and publish model cards and limitations.

### 8. [ ] Evidence retrieval and LangGraph triage — **High**

- Build source-linked lexical and semantic retrieval over narratives, report fields,
  FDA classifications, and recall context.
- Use FAISS as a reproducible local benchmark and evaluate `pgvector` for the public
  service.
- Add typed LangChain tools for retrieval, aggregation, comparison, and citation.
- Implement durable LangGraph states for routine, investigate, quarantine, and
  human-interrupt paths with replayable checkpoints.
- Evaluate an optional LLM only against the deterministic evidence-packet baseline.

### 9. [ ] Public research product — **High**

- Build a read-only FastAPI service and public Next.js interface.
- Provide search, trend exploration, cluster profiles, snapshot comparison, evidence
  inspection, methodology, and downloadable research extracts.
- Create authenticated reviewer queues and Tableau-ready gold tables from the same
  validated source of truth.
- Make citations, table views, accessibility, and limitation notices first-class UI
  requirements.

### 10. [ ] Production operations and release — **High**

- Check the FDA catalog regularly and ingest only when source metadata or content
  changes.
- Add resumable scheduling, retries, locks, data/model drift alerts, structured logs,
  metrics, run history, and last-known-good serving behavior.
- Add CI, deployment environments, security and privacy review, dependency scanning,
  backups, and restore drills.
- Publish methodology, architecture, data dictionary, model cards, contribution and
  security policies, and version every dataset/feature/model release.

## 📊 Planned analytical outputs

- Device-family and product-code reporting trends.
- Recurring narrative failure-mode clusters with representative source evidence.
- Reported-severity composition and change indicators.
- Novelty and volume-change indicators with confidence and data-quality context.
- Snapshot-to-snapshot diffs explaining which source records changed.
- Reproducible case packets for human investigation and export.

## 📁 Repository map

```text
src/maude/domain/       canonical models and enums
src/maude/ingestion/    archives, parsing, schemas, normalization, reconciliation
src/maude/quality/      blocking checks and rendered quality reports
src/maude/refresh/      FDA discovery, download, orchestration, and publication
src/maude/storage/      snapshot layout and DuckDB views
src/maude/service/      local source auditing
src/maude/cli.py        operator commands
tests/                  fixture-based unit and integration verification
docs/runbooks/          operating, failure, and recovery procedures
docs/superpowers/       approved design specification and implementation plans
```

## 📚 Design documentation

The complete product and scientific design is in
[`FDA MAUDE Safety-Signal and Triage Platform Design`](docs/superpowers/specs/2026-09-04-fda-maude-signal-triage-design.md).
The repository intentionally implements that design in independently verifiable
vertical slices.

## ⚖️ Responsible use

This software is for research and investigative triage. Its outputs are not medical
advice, regulatory findings, safety certifications, incidence estimates, or proof
that a device caused an outcome. Public deployment must retain these limitations and
must never expose sensitive data beyond what is lawful and appropriate from the
official source records.

<div align="center">

<sub>Official records in. Reproducible signals out. Human judgment remains in the loop.</sub>

</div>
