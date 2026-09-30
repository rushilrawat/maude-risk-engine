# MAUDE Risk Engine: Project Guide

This guide is the starting point for anyone joining the project. It explains the
goal, current code, data flow, important files, next work, and how to run the
project. Start here, then use the linked design and runbooks for detail.

## 1. What this project is

MAUDE Risk Engine is a research tool built around the U.S. FDA's public
Manufacturer and User Facility Device Experience (MAUDE) reports. The long-term
goal is to find recurring and changing patterns in reports about medical devices,
make the underlying reports easy to inspect, and help researchers prioritize
follow-up.

The system reports what was submitted. MAUDE is passive reporting data; it cannot
by itself establish incidence, causation, or comparative safety. Use “reporting
pattern,” “reported severity,” and “triage priority.” Do not turn counts or model
outputs into a claim that a device caused harm or is unsafe.

The intended public product combines reproducible analysis, a searchable evidence
corpus, a public research interface, Tableau-ready outputs, and a human review
queue. Statistical methods and deterministic rules remain the authority. An LLM
is optional and, if evaluated later, can only draft from a selected evidence packet
with citations.

## 2. Where the project stands

The current branch implements a strong ingestion foundation:

- Local archive audit and local snapshot ingestion.
- Discovery and download of the 12 current FDA archive files (four table families
  times base/add/change).
- Content-addressed source storage, SHA-256 manifests, and refresh run records.
- Streaming parsing and normalization into typed Parquet.
- Deterministic base/add/change reconciliation.
- Exact duplicate collapse and evidence-preserving quarantine of conflicting
  duplicate keys.
- Blocking quality checks, warning reports, and recoverable snapshot promotion.
- CLI commands: `audit-local`, `ingest-local`, and `refresh-fda`.

A real current-year refresh has been published locally as snapshot
`fda-current-43bedbf20c68c84b`. Its manifest is under
`MAUDE_DATA_ROOT/manifests/current.json`. All four canonical tables were produced.
The run records 34 conflicting patient rows quarantined, 50 device rejects,
1,164 master rejects, and low orphan-row warnings. These are review items and
should be carried into analysis; a promoted snapshot does not mean every row was
accepted or every warning resolved. The refresh evidence is local operator data,
not a repository fixture.

The next analytical stage is narrative preparation and feature engineering. There
is not yet an implemented TF-IDF/clustering model, signal score, evidence index,
LangGraph workflow, API, web app, scheduled production deployment, or reviewer UI.

## 3. Data flow and vocabulary

```text
FDA catalog and ZIP files
  -> raw: immutable ZIPs and preserved catalog responses, addressed by SHA-256
  -> bronze: streamed, parsed source rows and rejects
  -> silver: normalized Parquet tables for master/device/patient/narrative
  -> reconcile: base/add/change precedence, counts, and conflict evidence
  -> quality gate and manifest
  -> current pointer updated only after validated promotion
  -> future: report documents, features, models, retrieval, triage, and product
```

- **Raw:** bytes as received from FDA. A digest identifies content and avoids
  confusing a replaced source file with its old contents.
- **Bronze:** parsed source rows, retaining source columns and provenance.
- **Silver:** normalized, typed Parquet intended for downstream queries.
- **Manifest:** machine-readable record of source inputs, counts, quality results,
  snapshot state, and promotion evidence.
- **Reconciliation:** the source roles are applied as `change > add > base` for
  matching business keys. An absent row does not delete a prior row.
- **Conflict quarantine:** exact duplicate rows collapse. Different rows with the
  same business key are kept in `_conflicts` and omitted from selected records.
- **Current pointer:** `manifests/current.json` identifies the most recently
  promoted snapshot. A failed run should leave the previous pointer intact.

Storage location is controlled by `MAUDE_DATA_ROOT`. Raw data, large Parquet files,
and generated run outputs belong outside Git.

## 4. How to get oriented and run it

Requirements: Python 3.12 and `uv`.

```bash
uv sync --all-extras
cp .env.example .env
# Set MAUDE_DATA_ROOT in .env to a durable local output directory.
uv run maude version
make verify
```

Commands:

```bash
# Read-only audit of local canonical FDA archives
uv run maude audit-local --data-root /path/to/fda-archives

# Ingest locally held canonical archives after audit
uv run maude ingest-local local-2025-06 --source-root /path/to/fda-archives

# Discover and refresh the current official FDA files
uv run maude refresh-fda
```

The refresh command uses the configured `MAUDE_DATA_ROOT` and currently takes no
arguments. Read the runbook before a large download; it describes free-space
needs, output paths, failure recovery, and warnings.

## 5. Files to read first

| Purpose | Files |
|---|---|
| Product overview and roadmap | [`../README.md`](../README.md) |
| Complete design, scientific boundaries, model/retrieval/UI architecture | [`superpowers/specs/2026-09-04-fda-maude-signal-triage-design.md`](superpowers/specs/2026-09-04-fda-maude-signal-triage-design.md) |
| Current refresh design and safety requirements | [`superpowers/specs/2026-09-07-fda-current-refresh-design.md`](superpowers/specs/2026-09-07-fda-current-refresh-design.md) |
| Refresh implementation breakdown | [`superpowers/plans/2026-09-08-fda-current-refresh.md`](superpowers/plans/2026-09-08-fda-current-refresh.md) |
| Local ingestion operation | [`runbooks/local-ingestion.md`](runbooks/local-ingestion.md) |
| Current FDA refresh operation | [`runbooks/fda-current-refresh.md`](runbooks/fda-current-refresh.md) |
| CLI and public commands | [`../src/maude/cli.py`](../src/maude/cli.py) |
| Environment settings | [`../src/maude/config.py`](../src/maude/config.py), [`../.env.example`](../.env.example) |
| Contracts and run/snapshot models | [`../src/maude/domain/models.py`](../src/maude/domain/models.py), [`../src/maude/domain/enums.py`](../src/maude/domain/enums.py) |
| Archive discovery and downloads | [`../src/maude/ingestion/fda_catalog.py`](../src/maude/ingestion/fda_catalog.py), [`../src/maude/ingestion/download.py`](../src/maude/ingestion/download.py), [`../src/maude/ingestion/http.py`](../src/maude/ingestion/http.py) |
| Parse and normalize | [`../src/maude/ingestion/parser.py`](../src/maude/ingestion/parser.py), [`../src/maude/ingestion/normalize.py`](../src/maude/ingestion/normalize.py), [`../src/maude/ingestion/schemas.py`](../src/maude/ingestion/schemas.py) |
| Reconciliation and orchestration | [`../src/maude/ingestion/reconcile.py`](../src/maude/ingestion/reconcile.py), [`../src/maude/ingestion/refresh.py`](../src/maude/ingestion/refresh.py), [`../src/maude/ingestion/pipeline.py`](../src/maude/ingestion/pipeline.py) |
| Quality and manifests | [`../src/maude/quality/checks.py`](../src/maude/quality/checks.py), [`../src/maude/quality/report.py`](../src/maude/quality/report.py), [`../src/maude/ingestion/manifests.py`](../src/maude/ingestion/manifests.py) |
| Snapshot paths and query views | [`../src/maude/storage/layout.py`](../src/maude/storage/layout.py), [`../src/maude/storage/duckdb.py`](../src/maude/storage/duckdb.py) |
| Verification commands | [`../Makefile`](../Makefile), [`../pyproject.toml`](../pyproject.toml), [`../tests/`](../tests/) |
| Remaining GitHub issue drafts | [`ISSUE_BACKLOG.md`](ISSUE_BACKLOG.md) |

Suggested reading order: this guide, README, current refresh runbook, CLI, refresh
orchestrator, ingestion pipeline, reconciliation, quality checks, then the full
design spec.

## 6. Next implementation stage: narrative features

The first modeling unit should be a report with its narrative evidence retained as
separate source-linked rows. `src/maude/storage/duckdb.py` already exposes a
`report_documents` view with report fields, device metadata, narrative text, and
source row identifiers. Begin by auditing that view against the modeling contract
in section 9 of the design spec.

Recommended sequence:

1. Define a stable report-document record and cleaning rules that preserve
   negations, measurements, device codes, and source text.
2. Build a deterministic narrative cleaning and document-construction step,
   retaining `mdr_text_key`, text type, file, line, and snapshot provenance.
3. Add small, de-identified or synthetic fixtures for punctuation, Unicode,
   boilerplate, repeated narratives, and conflicting evidence. Do not commit raw
   FDA data or generated corpora.
4. Add a versioned word and character TF-IDF baseline. Fit preprocessing and
   vocabulary only on training data.
5. Evaluate feature behavior and cohort choices before adding clustering.
6. Implement clustering and time-based evaluation as separate versions after the
   baseline document/feature outputs are stable.

Read the matching issue in [`ISSUE_BACKLOG.md`](ISSUE_BACKLOG.md) before taking a
task. Each issue draft lists likely files, dependencies, and acceptance criteria.

## 7. Working agreements for a new contributor

- Start from the repository's active feature branch or an agreed fresh branch; do
  not implement directly on `main`.
- Before code changes, check `git status` and preserve changes already in the
  checkout.
- Keep raw data, secrets, local `.env`, generated Parquet, indexes, and model files
  out of Git.
- Keep ingestion deterministic, bounded-memory, and provenance-preserving.
- For code changes, add focused tests and run `make verify` before proposing a
  merge.
- Keep each pull request to one issue-sized change; update docs and issue status
  when behavior changes.
- Do not add an LLM dependency to the statistical/data pipeline. Retrieval and
  orchestration come after an evidence packet and baseline analysis are reliable.

## 8. Decision record and open questions

- **Canonical source:** official FDA sources only for the initial scope.
- **Storage today:** local immutable files and Parquet, queried with DuckDB.
- **Future serving:** Postgres/pgvector, API, browser UI, and Tableau are design
  targets, not current capabilities.
- **Model baseline:** word and character TF-IDF followed by evaluated
  MiniBatchKMeans; embedding retrieval is a later comparison.
- **LLM:** optional, evidence-constrained report drafting only after deterministic
  retrieval and human review are measured.
- **Unknown until implementation:** final hosting, user authentication provider,
  scheduler, review roles, and exact public deployment topology.
