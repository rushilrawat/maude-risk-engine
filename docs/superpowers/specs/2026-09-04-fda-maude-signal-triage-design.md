# FDA MAUDE Safety-Signal and Triage Platform Design

**Status:** Proposed for user review  
**Date:** 2026-09-04  
**Working product name:** MAUDE Signal Explorer

## 1. Outcome

Build a publicly deployed, research-grade platform that continuously ingests official FDA medical-device reporting data, identifies recurring and emerging reporting patterns with reproducible data-science methods, retrieves source-linked historical evidence, and routes noteworthy cases through a persistent human-review workflow.

The analytical system must be fully useful without a large language model. Scikit-learn models and transparent rules remain authoritative. LangGraph manages stateful triage, branching, failure recovery, and human review. LangChain supplies typed retrieval/tool interfaces so an evidence-constrained LLM summarizer can be evaluated later without redesigning the system.

The same validated data products feed:

1. a public web application;
2. reproducible research notebooks and downloadable extracts;
3. a Tableau dashboard; and
4. an authenticated human-review queue.

## 2. Scientific Positioning

The product is a **reporting-signal exploration and triage system**, not a clinical risk calculator.

FDA describes MAUDE as a passive surveillance system and states that it cannot, by itself, establish incidence, prevalence, causality, changes in event rates, or comparative device risk. Reports may be incomplete, inaccurate, duplicated, delayed, unverified, or affected by reporting and publicity bias. The platform therefore uses the following language consistently:

- “report count,” never “number of harmed patients” unless the record explicitly supports that statement;
- “reporting signal” or “pattern,” never “proven safety problem”;
- “triage priority,” never “probability that a device is dangerous”;
- “associated with,” never “caused by”; and
- “reported severity,” never inferred clinical severity.

Every public analytical page displays the snapshot date, model version, evidence links, and a concise limitations notice. The methodology page contains the complete limitations statement.

## 3. Scope

### 3.1 Included in the first production release

- Official FDA MAUDE bulk files as the canonical adverse-event source.
- Historical bootstrap plus automatic discovery of current-year base, add, and change files.
- Master event, device, patient, narrative, device-problem, and patient-problem tables.
- FDA device classification metadata.
- FDA device recall data as contextual evidence, clearly distinguished from MAUDE reports.
- Immutable source snapshots, row-level provenance, and correction history.
- Narrative preprocessing, TF-IDF features, dimensionality reduction where validated, and MiniBatchKMeans clustering.
- Transparent severity, novelty, confidence, and report-volume-change indicators.
- Semantic retrieval over source narratives with citations.
- Stateful LangGraph triage with a durable Postgres checkpointer and human interrupts.
- FastAPI backend, TypeScript/Next.js public interface, and authenticated reviewer views.
- Tableau-ready extracts from the same gold analytical tables.
- Scheduled refreshes, monitoring, data-quality gates, automated tests, and documented model evaluations.

### 3.2 Explicitly excluded from the first release

- Clinical recommendations or diagnosis.
- A claim that report volume estimates incidence or device risk.
- Autonomous closure of severe or ambiguous cases.
- External news, social media, patient forums, or commercial datasets.
- Automatic public “safety alerts.” Only FDA can provide authoritative FDA safety communications.
- A general-purpose chat agent.
- Model training during an API request.
- Raw data or generated artifacts committed to Git.

### 3.3 Optional evaluated extension

An LLM may later draft a cited investigation brief from an already-selected evidence packet. It cannot calculate signal indicators, choose supporting evidence silently, overwrite source text, determine causality, or finalize a case. The non-LLM case packet remains the baseline and fallback.

## 4. Source Strategy and Refresh Cadence

### 4.1 Canonical source

The [FDA MDR Data Files page](https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files#download) is the source catalog. FDA currently describes these pipe-delimited ZIP files as monthly updated and states that record types join through `MDR_REPORT_KEY`.

The downloader discovers links only from approved FDA hosts, validates expected filename families, records the source page retrieval time, and stores an immutable manifest containing:

- source URL and HTTP metadata;
- download start and completion timestamps;
- SHA-256 checksum;
- compressed and uncompressed byte counts;
- archive member names;
- detected encoding;
- parsed header and column count;
- parsed, accepted, rejected, inserted, updated, and unchanged row counts;
- minimum and maximum applicable dates;
- loader version and schema version; and
- run identifier and terminal status.

The scheduler checks the source catalog daily because FDA may replace files without a predictable local execution date. It starts an ingestion run only when a source URL, ETag, Last-Modified value, size, or checksum changes. This delivers the newest available official data while respecting the source’s monthly publication cadence.

### 4.2 Bulk-file semantics

- Historical “through year” and annual files bootstrap the corpus.
- Current-year base files populate the current-year working set.
- `add` files contribute newly published records.
- `change` files update existing records and may add child records to an existing report.
- An annual rollover is treated as a controlled re-bootstrap of the completed year, followed by reconciliation against previously ingested current-year snapshots.

Updates never erase history. Each normalized row has a stable business key, a source snapshot identifier, a content hash, `valid_from`, `valid_to`, and `is_current`. Public queries use current rows; research queries can reconstruct any previous snapshot.

### 4.3 Supplementary FDA sources

The platform uses official openFDA endpoints for device classification and recall enrichment and for freshness reconciliation. The bulk MAUDE files remain canonical for reproducible adverse-event analysis. API responses are cached with retrieval metadata rather than treated as timeless facts.

Recall context is presented as a separate FDA record type. It is not interpreted as confirmation that a MAUDE cluster caused a recall. openFDA’s own device-recall documentation cautions against using the endpoint to issue public alerts or track the complete recall lifecycle.

### 4.4 Source migration

FDA states that MAUDE data will become available through the Adverse Event Monitoring System in 2026. Source-specific discovery, download, and schema translation therefore live behind a `ReportSourceAdapter` interface. A future AEMS adapter can emit the same canonical records without changing feature generation, modeling, triage, APIs, or dashboards.

## 5. Existing Local Data Assessment

The repository currently contains raw June 2025 downloads and converted text files but no application code or Git repository.

Observed local row counts, excluding headers, are approximately:

| File | Rows | Observation |
|---|---:|---|
| `mdrfoi_UTF8.txt` | 1,059,988 | Current-year master records |
| `mdrfoiAdd_UTF8.txt` | 175,063 | Monthly master additions |
| `DEVICE_UTF8.txt` | 1,059,658 | Current-year device records |
| `foitext_UTF8.txt` | 2,259,460 | Current-year narratives |
| `foitextAdd_UTF8.txt` | 374,800 | Monthly narrative additions |
| `patient.txt` | 1,070,300 | Original patient file |
| `patient_UTF8.txt` | 2,974 | Truncated conversion; must not be trusted |

The first implementation task preserves these files as an input fixture, verifies them against the ZIP archives, and regenerates UTF-8 during ingestion. Hand-converted `*_UTF8.txt` files are never assumed complete merely because they decode successfully.

The local snapshot is suitable for a vertical slice, but the production bootstrap must download a consistent current set of all required table families and historical archives from FDA.

## 6. System Architecture

```text
FDA bulk files / official FDA APIs
               |
               v
      source discovery + manifests
               |
               v
    immutable ZIP snapshots (raw/bronze)
               |
               v
 streaming parse + schema/data-quality gate
               |
               v
 versioned canonical Parquet tables (silver)
               |
       +-------+--------------------+
       |                            |
       v                            v
 feature/model pipeline       Postgres current views
       |                            |
       v                            |
 clusters + signal facts            |
       |                            |
       +-------------+--------------+
                     v
          evidence retrieval index
                     |
                     v
         LangGraph triage workflow
          /        |         \
     routine   investigate   quarantine
        |           |             |
        |      evidence packet    data repair
        |           |
        +------ human interrupt
                     |
          reviewed disposition
                     |
        +------------+-------------+
        |            |             |
     FastAPI      Tableau       research exports
        |
     Next.js UI
```

Batch ingestion and model jobs use a workflow scheduler such as Prefect. LangGraph is deliberately not the ETL scheduler. It is used only where each case has durable state, runtime routing, selective tool execution, and an indefinite human-review pause.

## 7. Storage Layers

### 7.1 Raw/bronze

Original ZIP files and manifests are immutable. Development uses a local directory with the same key layout as production object storage:

```text
data/raw/fda-maude/<snapshot-date>/<source-filename>
data/manifests/fda-maude/<run-id>.json
```

### 7.2 Canonical/silver

Parsing writes partitioned Parquet using PyArrow and DuckDB. Pandas is used for bounded exploratory and modeling frames, not for loading multi-gigabyte files into memory in one operation.

Canonical entities are:

- `reports`: one versioned master-report row per FDA master business key;
- `devices`: one versioned row per report/device sequence;
- `patients`: one versioned row per report/patient sequence;
- `narratives`: one versioned row per `MDR_TEXT_KEY` with text type preserved;
- `device_problems`: report/device/problem-code relationships;
- `patient_problems`: report/patient/problem-code relationships;
- `device_classifications`: FDA product-code metadata;
- `recalls`: official recall context; and
- `source_records`: provenance and rejection details.

Dates are parsed into typed columns while raw strings are retained. Empty strings, FDA sentinel values, malformed dates, and genuinely missing values remain distinguishable.

### 7.3 Analytical/gold

Gold tables are versioned by `dataset_snapshot_id`, `feature_version`, and `model_version`:

- `report_documents`;
- `report_features`;
- `cluster_assignments`;
- `cluster_profiles`;
- `cluster_monthly_counts`;
- `report_signal_indicators`;
- `triage_cases`;
- `triage_transitions`;
- `evidence_links`;
- `review_decisions`; and
- `data_refresh_status`.

### 7.4 Serving database

PostgreSQL stores current public views, review records, graph checkpoints, and vector-search metadata. The `pgvector` extension serves production semantic retrieval. FAISS is retained as a reproducible local benchmark and offline research index, which preserves the project’s FAISS experimentation without making a process-local file the public system’s source of truth.

## 8. Data Quality and Failure Handling

An ingestion run is published only after all blocking checks pass:

- checksum and archive integrity;
- expected header signature and field count;
- encoding conversion without silent truncation;
- row-count reconciliation against the manifest and prior snapshot;
- uniqueness of each table’s composite business key within a source version;
- accepted event-type and flag domains;
- child-to-report referential-integrity thresholds;
- valid date parsing and plausible ranges;
- duplicate and update accounting;
- narrative-length distribution drift; and
- source freshness.

A schema change, sudden unexplained row-count reduction, missing required table family, or decoding truncation quarantines the run. The last validated public snapshot remains live. Rejected records are preserved with error reason, source file, byte/line location where available, and parser version.

Warnings such as optional-field completeness changes do not block publication but appear in the run report. Every data-quality threshold is version-controlled and tested against small golden fixtures, including malformed delimiters, CRLF input, Latin-1 characters, duplicate rows, changed rows, and the observed truncated-patient-file failure.

## 9. Narrative and Feature Pipeline

### 9.1 Document construction

The model unit is an FDA report, while evidence remains traceable to individual narrative rows. A report document contains:

- selected narrative sections in a stable order;
- text-type and source-row boundaries;
- product code, generic and brand names, manufacturer, model number, and event type as metadata;
- source snapshot and evidence identifiers; and
- no patient demographics in the text representation unless a defined research experiment requires them.

### 9.2 Cleaning

Cleaning normalizes Unicode, whitespace, control characters, and obvious formatting artifacts. It does not remove negations, measurements, error codes, model numbers, or clinically meaningful punctuation blindly.

Repeated regulatory disclaimers and manufacturer boilerplate are detected from corpus frequency and near-duplicate templates. They are masked in the modeling representation but retained in the source evidence. Exact and near-duplicate narratives receive duplicate-group identifiers so repeated submissions cannot dominate evaluation unnoticed.

### 9.3 Classical features

The first benchmark combines word and character TF-IDF:

- word n-grams capture interpretable failure phrases;
- character n-grams tolerate abbreviations, spelling variation, and device codes;
- `min_df`, `max_df`, vocabulary size, and n-gram ranges are selected only from training data; and
- TruncatedSVD is evaluated against sparse direct clustering rather than assumed beneficial.

Models are trained within sufficiently populated FDA product-code or device-family cohorts, with a documented global fallback for sparse cohorts. This prevents clusters from merely rediscovering broad device categories.

### 9.4 Clustering

MiniBatchKMeans is the production baseline because it scales to the corpus and supports assignment of new monthly reports. Candidate model configurations are compared using:

- stability across at least five random seeds and bootstrap samples;
- approximate silhouette and centroid separation;
- cluster-size balance and outlier coverage;
- top-term coherence;
- duplicate sensitivity;
- temporal stability; and
- blinded human review of representative and boundary reports.

The chosen model is serialized with its vectorizer, optional reducer, centroids, cohort definition, training date range, dataset snapshot, environment lock hash, evaluation report, and generated model card.

No cluster is assigned a medical interpretation automatically. Cluster display labels combine representative terms with an explicitly machine-generated label marker until reviewed.

## 10. Signal Indicators and Routing

### 10.1 Indicators

Each incoming report receives independently inspectable indicators:

- **reported severity:** directly mapped from FDA event type, preserving unknown values;
- **cluster confidence:** calibrated distance and nearest-versus-second-nearest centroid margin within the relevant model;
- **novelty:** distance percentile relative to a held-out historical calibration distribution;
- **report-volume change:** recent monthly count deviation for the same cohort/cluster compared with its own prior history; and
- **evidence density:** number and diversity of comparable historical reports.

Report-volume change is never labeled incidence or risk. The first method compares the most recent complete three-month average with the preceding twelve complete months using a robust median/MAD baseline, requires at least five additional reports, and emits the raw counts beside the standardized indicator. Later statistical methods must outperform this transparent baseline in time-split evaluation before replacing it.

### 10.2 Initial deterministic routing policy

The initial policy is versioned and produces both a route and machine-readable reasons:

1. Missing required text or invalid core fields → `quarantine`.
2. Reported death → `investigate` with highest queue priority.
3. Reported injury plus novelty at or above its cohort’s 95th calibration percentile → `investigate`.
4. Novelty at or above the 99th percentile → `investigate`.
5. Report-volume-change robust z-score at or above 3 with the minimum count increase → all new reports in that signal group enter `investigate`.
6. Cluster-confidence margin at or below its cohort’s 10th calibration percentile plus fewer than five close historical neighbors → `investigate`.
7. All other valid reports → `routine`.

These are triage thresholds, not safety conclusions. Calibration reports show routing volume, cohort distribution, severity distribution, false-positive review burden, and sensitivity to threshold changes. Thresholds can change only by creating a new policy version.

## 11. Evidence Retrieval

### 11.1 Retrieval corpus

Each narrative or coherent narrative section is represented as a LangChain `Document` containing the exact evidence identifier and filterable metadata. A sentence-transformer creates embeddings in versioned offline batches. Production vectors live in pgvector; FAISS provides an offline equivalence and performance benchmark.

### 11.2 Hybrid retrieval

Investigation uses a deterministic query assembled from report text and structured fields. Retrieval combines:

- semantic vector similarity;
- lexical/BM25 similarity;
- cohort filters for product code or device family;
- optional manufacturer/model filters;
- event-type and date filters; and
- duplicate-group diversity.

Results are reranked with a transparent weighted reciprocal-rank fusion baseline. The evidence packet contains similarity scores, filter reasons, source text excerpts, report identifiers, snapshot identifiers, and direct FDA record/search links when stable links are available.

### 11.3 Retrieval evaluation

A labeled benchmark contains at least 100 query reports with reviewer-selected related cases across common, ambiguous, and novel examples. Metrics include recall@5, recall@10, mean reciprocal rank, cohort-filter accuracy, duplicate diversity, latency, and citation integrity. The initial production gate is recall@10 of at least 0.80, an improvement of at least 0.05 absolute over lexical-only retrieval, 100% resolution of returned evidence identifiers to the indexed source snapshot, and p95 retrieval latency below 1.0 second for 50 concurrent read-only users. If semantic retrieval misses this gate, the public product uses lexical retrieval while the embedding experiment remains available only in research outputs.

## 12. LangGraph Triage Workflow

### 12.1 Why LangGraph is warranted

The graph provides runtime-dependent branches, selective evidence tools, durable checkpoints, retries, an indefinite human interrupt, and resumable audit history. A normal function pipeline would require custom infrastructure for these properties. LangGraph’s documented persistence and interrupt model directly supports this workflow.

### 12.2 State contract

Graph state contains identifiers and JSON-serializable values, never DataFrames or full model objects:

```python
class TriageState(TypedDict):
    case_id: str
    report_id: str
    dataset_snapshot_id: str
    feature_version: str
    model_version: str
    policy_version: str
    reported_severity: str | None
    cluster_id: str | None
    cluster_confidence: float | None
    novelty_percentile: float | None
    volume_change_score: float | None
    evidence_density: int | None
    route: Literal["routine", "investigate", "quarantine"] | None
    route_reasons: list[str]
    evidence_ids: list[str]
    case_packet_id: str | None
    review_status: Literal[
        "not_required", "pending", "reviewed_closed", "reviewed_escalated"
    ] | None
    reviewer_notes: str | None
    errors: list[dict[str, str]]
    tool_events: list[dict[str, str]]
```

Large evidence bodies live in normal database tables and are referenced by ID. A Postgres checkpointer stores graph snapshots under `case_id` as the thread identifier.

### 12.3 Nodes and branches

```text
START
  -> validate_report
      -> quarantine_case -> END                         [invalid]
      -> classify_report                               [valid]
           -> calculate_indicators
                -> choose_route
                    -> file_routine -> END              [routine]
                    -> retrieve_similar_cases           [investigate]
                         -> fetch_device_history
                         -> fetch_signal_history
                         -> assemble_case_packet
                         -> optional_draft_summary
                         -> await_human_review           [interrupt]
                              -> close_review -> END
                              -> escalate_review -> END
```

Independent history tools may execute in parallel after retrieval. Each tool has a typed input/output schema, timeout, retry policy, and idempotency key. Exhausted failures produce a partial packet marked incomplete and keep the case reviewable; they never silently route the case to routine.

### 12.4 LangChain’s role

LangChain provides `Document`, retriever, and typed `@tool` contracts for:

- `find_similar_reports`;
- `get_device_history`;
- `get_signal_history`;
- `get_recall_context`; and
- `get_source_evidence`.

The graph invokes these tools deterministically in the first release. There is no claim that a model autonomously chose them. If an LLM is added later, the same interfaces can support constrained tool calling and grounded generation.

### 12.5 Human review

The graph interrupts after the evidence packet is assembled. An authenticated reviewer can:

- close the case as an explained/known reporting pattern;
- escalate it as a research signal requiring further analysis;
- request corrected evidence retrieval; or
- record that the available evidence is insufficient.

Reviewer identity, timestamp, state version, reason codes, notes, and evidence viewed are stored. Public pages never represent a pending machine route as a human-reviewed conclusion.

## 13. Optional LLM Experiment

The first release renders an extractive case packet with structured facts, representative excerpts, and citations. This is the required baseline.

The optional experiment adds one node, `draft_summary`, after evidence assembly. Its input is only the evidence packet. Its output follows a schema with:

- observed failure pattern;
- reported outcomes;
- similarities across retrieved cases;
- meaningful differences and contradictions;
- missing information;
- a non-causal interpretation warning; and
- sentence-level evidence identifiers.

Evaluation uses a blinded sample of at least 100 cases. Reviewers compare the extractive baseline and LLM draft for citation correctness, unsupported claims, omitted contradictory evidence, usefulness, editing time, and preference. The LLM mode is disabled by default unless it achieves 100% valid citation identifiers, no critical unsupported claims in the test set, and a measurable reduction in median reviewer preparation time. The extractive packet remains available for every case.

## 14. Public Product Experience

### 14.1 Public pages

1. **Latest signals:** recent reporting-pattern changes, novel clusters, reported severity composition, and data-current-through status.
2. **Search:** device, manufacturer, product code, model, report number, narrative terms, event type, and date filters.
3. **Device/product-code profile:** report counts over time, clusters, severity mix, recall context, and data limitations.
4. **Cluster profile:** reviewed or machine-generated label, representative terms, timeline, cohort, representative reports, and model/version details.
5. **Report evidence:** structured FDA fields, narrative sections, cluster assignment, related reports, and canonical source provenance.
6. **Methods and limitations:** source coverage, update history, model cards, evaluation results, and appropriate-use guidance.
7. **System status:** last successful refresh, current snapshot, source freshness, and known data-quality warnings.

The home page leads with recent signals because freshness is the project’s main public value. Device search and cluster exploration remain primary navigation items.

### 14.2 Reviewer pages

Authenticated pages show the prioritized queue, case packet, graph transition history, evidence, reviewer decision form, and requests for evidence refresh. Public users cannot mutate triage state.

### 14.3 API

FastAPI exposes versioned, read-only public endpoints for snapshots, search, reports, devices, clusters, signals, and methodology metadata. Reviewer endpoints require authentication and role checks. Cursor pagination, bounded filters, response schemas, caching, and rate limiting protect the service.

The API returns a `provenance` object with snapshot/model versions and makes source evidence IDs part of every analytical response.

## 15. Tableau Integration

The export job reads only validated gold tables and produces stable extracts for:

- monthly reporting-pattern trends;
- severity composition;
- manufacturer/device/product-code exploration;
- cluster frequency and novelty;
- triage-route and review outcomes; and
- pipeline freshness and quality.

Every extract includes refresh timestamp, snapshot identifier, model version, cohort definition, and limitations text. Tableau calculations do not reimplement scientific metrics; they visualize metrics computed and tested in Python.

## 16. Application and Repository Structure

```text
MAUDE/
├── pyproject.toml
├── uv.lock
├── .env.example
├── docker-compose.yml
├── Makefile
├── src/maude/
│   ├── config.py
│   ├── domain/
│   │   ├── models.py
│   │   └── enums.py
│   ├── ingestion/
│   │   ├── adapters/base.py
│   │   ├── adapters/fda_bulk.py
│   │   ├── discovery.py
│   │   ├── downloader.py
│   │   ├── parser.py
│   │   ├── normalize.py
│   │   └── manifests.py
│   ├── quality/
│   │   ├── checks.py
│   │   └── reports.py
│   ├── storage/
│   │   ├── parquet.py
│   │   ├── duckdb.py
│   │   └── postgres.py
│   ├── features/
│   │   ├── documents.py
│   │   ├── cleaning.py
│   │   └── tfidf.py
│   ├── modeling/
│   │   ├── clustering.py
│   │   ├── evaluation.py
│   │   ├── indicators.py
│   │   └── registry.py
│   ├── retrieval/
│   │   ├── embeddings.py
│   │   ├── index.py
│   │   ├── hybrid.py
│   │   └── evaluation.py
│   ├── triage/
│   │   ├── state.py
│   │   ├── tools.py
│   │   ├── nodes.py
│   │   ├── routing.py
│   │   └── graph.py
│   ├── api/
│   │   ├── app.py
│   │   ├── dependencies.py
│   │   └── routes/
│   ├── exports/tableau.py
│   └── jobs/
│       ├── refresh.py
│       ├── train.py
│       └── triage.py
├── web/
│   ├── app/
│   ├── components/
│   └── tests/
├── migrations/
├── notebooks/
│   ├── 01_data_profile.ipynb
│   ├── 02_cluster_selection.ipynb
│   └── 03_signal_backtest.ipynb
├── tests/
│   ├── fixtures/
│   ├── unit/
│   ├── integration/
│   ├── graph/
│   ├── model/
│   └── api/
├── docs/
│   ├── architecture/
│   ├── methodology/
│   ├── model-cards/
│   └── runbooks/
└── data/                 # ignored; manifests may be copied into docs/releases
```

Python 3.12+, strict Pydantic models, Ruff, mypy, pytest, and pinned dependencies form the baseline. SQL migrations manage PostgreSQL. The web app uses TypeScript with generated API types.

## 17. Deployment and Operations

Local development uses Docker Compose for PostgreSQL/pgvector, API, worker, and web services. Production deploys the same containers with managed PostgreSQL and S3-compatible object storage.

Heavy parsing, embedding, and training run in background workers. The API serves precomputed results and bounded retrieval only. A validated snapshot is promoted atomically so users never observe half-refreshed tables.

Operational telemetry includes:

- structured logs carrying run, snapshot, model, case, and request identifiers;
- ingestion duration and source freshness;
- parsed/rejected/inserted/updated counts;
- graph routes, interrupts, retries, and failures;
- API latency, error rate, cache hit rate, and slow queries;
- retrieval latency and empty-result rate; and
- model/routing distribution drift.

Alerts fire for failed scheduled runs, stale public data, schema drift, row-count collapse, quality-gate failure, graph backlog, and serving errors. Runbooks document recovery without overwriting the last valid snapshot.

## 18. Security, Privacy, and Responsible Access

- Only public FDA data is ingested.
- The system does not attempt re-identification or enrichment of patient identities.
- Raw narratives are treated as untrusted text and escaped before rendering.
- Downloaded archives are size-limited, checksum-verified, and extracted without trusting member paths.
- Public APIs are read-only, rate-limited, paginated, and protected against expensive arbitrary queries.
- Reviewer mutations require authentication, role authorization, CSRF protection where applicable, and an immutable audit record.
- Secrets are injected at runtime and never stored in source manifests, logs, notebooks, or browser bundles.
- LLM use, if enabled, sends only the minimum public evidence packet and is recorded by provider/model/prompt version.

## 19. Verification Strategy

### 19.1 Software tests

- Unit tests for parsing, normalization, keys, indicators, routing, and API schemas.
- Golden-file tests for every FDA table family and encoding edge case.
- Property tests for idempotent ingestion and version-history invariants.
- Integration tests from ZIP fixture through Parquet and PostgreSQL.
- Graph tests forcing routine, investigate, quarantine, retry, partial-evidence, interrupt, resume, close, and escalate paths.
- API contract, authorization, pagination, and provenance tests.
- Browser tests for primary public and reviewer journeys.
- Load tests on search and high-traffic signal pages.
- Reproducibility tests confirming the same snapshot/config/seed yields identical assignments and metrics.

### 19.2 Research validation

The initial time split is:

- training: reports received through 2024;
- model and threshold validation: 2025 reports; and
- untouched prospective-style test: available 2026 reports.

All vocabulary building, duplicate-template discovery, cohort thresholds, centroids, and calibration distributions use only their allowed period. Evaluations report results by product-code cohort, report source, event type, and time.

Human review uses a documented rubric and a stratified sample containing representative, boundary, novel, severe, and duplicate-heavy reports. Disagreements and uncertainty are retained rather than forced into false ground truth.

### 19.3 Release gates

A public release requires:

- a fully reproducible clean bootstrap;
- all blocking data-quality checks passing;
- no critical security findings in dependency and application scans;
- graph path and interrupt/resume tests passing;
- model and retrieval evaluation reports published;
- public limitations and freshness visible;
- rollback to the previous snapshot tested; and
- a smoke test against the deployed API and UI.

## 20. Delivery Roadmap

### Phase 0 — Repository and data safety

Initialize source control, establish ignored data paths, create the Python package and test harness, preserve raw archives, and document commands. Build a tiny deterministic fixture from the local files without committing source narratives unnecessarily.

**Exit:** clean checkout can install dependencies and run a fixture test; raw multi-gigabyte files cannot be accidentally committed.

### Phase 1 — Local snapshot audit and vertical ingestion slice

Profile all local archives, reproduce encoding conversion, prove the patient UTF-8 truncation is detected, parse master/device/patient/narrative rows, and emit manifests plus Parquet for a bounded sample and then the full local snapshot.

**Exit:** repeat runs are idempotent; table counts and rejects reconcile; `MDR_REPORT_KEY` joins work; local snapshot quality report is generated.

### Phase 2 — Canonical FDA refresh pipeline

Implement the FDA source adapter, immutable downloader, all table families, base/add/change reconciliation, historical bootstrap, current views, scheduler, and atomic publication.

**Exit:** an unchanged source produces no mutation; a changed fixture creates correct versions; a source/schema failure leaves the last validated snapshot live.

### Phase 3 — Research document and TF-IDF baseline

Construct report documents, detect duplicate boilerplate, create cohort rules, train word/character TF-IDF baselines, compare clustering configurations, and publish evaluation artifacts plus model cards.

**Exit:** selected clustering is reproducible, more coherent than declared baselines, stable across seeds, and inspectable through representative reports.

### Phase 4 — Reporting-signal indicators and backtest

Implement severity, confidence, novelty, evidence-density, and report-volume-change indicators. Run 2024/2025/2026 time-split evaluations and sensitivity analysis. Version the initial deterministic triage policy.

**Exit:** every indicator can be reconstructed from stored inputs; the routing split is non-degenerate and reported by cohort; terminology does not imply incidence or causality.

### Phase 5 — Hybrid evidence retrieval

Create sentence embeddings, FAISS research benchmark, pgvector production index, lexical retrieval, reciprocal-rank fusion, metadata filters, and citation-integrity checks. Build and evaluate the 100-query benchmark.

**Exit:** production retrieval meets the numeric recall, improvement, citation-integrity, and latency gates in Section 11.3, and every returned passage resolves to an immutable source record.

### Phase 6 — LangGraph triage and human review

Implement typed LangChain tools, state schema, graph nodes, conditional edges, Postgres checkpointing, retries, partial evidence behavior, interrupts, resume decisions, and transition audit tables.

**Exit:** tests demonstrate every genuine branch; a process restart can resume a pending review; no severe/ambiguous case is silently auto-closed.

### Phase 7 — API and public/reviewer web application

Expose snapshot, search, report, device, cluster, signal, evidence, and methodology APIs. Build the public latest-signals experience and authenticated review queue.

**Exit:** core user journeys pass browser and accessibility tests; every analytical view shows freshness, provenance, and limitations.

### Phase 8 — Tableau and research deliverables

Generate stable gold extracts, build Tableau views, finish notebooks, methodology documentation, model cards, data dictionary, and reproducibility guide.

**Exit:** Tableau and the web app reconcile to the same snapshot and metrics; a researcher can reproduce the published evaluation from documented commands.

### Phase 9 — Production hardening and launch

Containerize, deploy, configure scheduler/worker/storage/database, add observability and alerts, load-test, security-test, exercise rollback, and publish system status.

**Exit:** two consecutive refresh cycles complete without manual repair, failure injection preserves the previous snapshot, and deployed smoke tests pass.

### Phase 10 — Optional grounded-summary experiment

Add the constrained LLM draft node behind a feature flag, run blinded comparison against extractive packets, publish results, and enable it only if it passes the stated quality gates.

**Exit:** either the feature is enabled with evidence that it helps, or it remains disabled with the negative result documented. Both outcomes preserve a complete production product.

## 21. First Two-Week Starting Sequence

The first iteration should produce a narrow end-to-end vertical slice rather than isolated scaffolding:

1. Initialize Git and Python tooling; add data safeguards.
2. Create representative golden fixtures from all locally available record types.
3. Implement manifest and encoding/archive validation.
4. Stream-parse master, device, patient, and narrative fixtures.
5. Write canonical Parquet and query it with DuckDB.
6. Join one report document with exact source provenance.
7. Train a small TF-IDF/MiniBatchKMeans baseline on a bounded cohort.
8. Persist cluster assignment and transparent distance metrics.
9. Implement a minimal LangGraph with `validate → classify → route → routine/investigate`.
10. Prove both routes with real local records and persist transition logs.
11. Render a non-LLM evidence packet for one investigated case.
12. Publish the iteration’s data-quality report and model evaluation.

This slice deliberately postpones the public UI, embeddings, and optional LLM. It validates the hardest contracts—source provenance, model assignment, real branching, and reproducibility—before expanding the system.

## 22. Success Measures

### Engineering

- Every public record and metric resolves to a source snapshot.
- Reprocessing identical input produces no duplicate current records.
- A failed refresh never corrupts or partially replaces the public snapshot.
- Graph interrupts survive worker restart and resume under the same case ID.
- With 50 concurrent read-only users against a production-sized index, p95 latency is below 500 ms for cached profile/detail endpoints, below 1.5 seconds for filtered text search, and below 2.0 seconds for an uncached signal page.

### Data science

- On the selected cohorts, clustering reaches median pairwise adjusted Rand index of at least 0.65 across the required seed/bootstrap runs, and reviewers judge at least 80% of a stratified set of 100 sampled assignments coherent with their displayed cluster; cohorts that miss either gate remain research-only.
- Thresholds are calibrated on past data and evaluated on untouched future-period data.
- Routing is non-degenerate and its review burden is quantified by cohort.
- Retrieval improves recall@10 over lexical-only retrieval and preserves exact citations.
- Results explicitly quantify uncertainty, missingness, duplication, and reporting bias.

### Product and research

- Users can discover a current reporting signal, inspect its history, and reach source evidence.
- Researchers can reproduce a published cluster/evaluation from a versioned snapshot and config.
- Tableau and web metrics reconcile exactly.
- Methodology and limitations are visible without requiring repository access.
- Resume claims are updated only with measured corpus size, routing split, evaluation results, and deployed capabilities.

## 23. Decision Summary

- FDA-first and official-source-only for the first release.
- Bulk MAUDE files are canonical; openFDA enriches and reconciles.
- Recency means automated discovery and versioned monthly publication, not unsupported real-time claims.
- DuckDB/Parquet handle analytical scale; PostgreSQL/pgvector handle serving and graph state.
- Pandas remains part of bounded analysis; scikit-learn owns classical modeling.
- MiniBatchKMeans is the first clustering baseline, selected through evaluation rather than assumed optimal.
- “Triage priority” replaces “risk score” in scientific and public language.
- LangGraph manages durable case routing and human review, not ETL.
- LangChain standardizes retrieval and tools; it does not justify an “agentic AI” claim by itself.
- The production baseline has no LLM dependency.
- Any LLM is a later, evidence-constrained summarization experiment with a non-LLM control.

## 24. Authoritative References

- [FDA MDR Data Files](https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files)
- [FDA Medical Device Reporting overview and limitations](https://www.fda.gov/medical-devices/medical-device-safety/medical-device-reporting-mdr-how-report-medical-device-problems)
- [About the MAUDE Database](https://www.fda.gov/medical-devices/mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/about-manufacturer-and-user-facility-device-experience-maude-database)
- [openFDA device adverse-event API](https://open.fda.gov/apis/device/event/)
- [openFDA device recall API](https://open.fda.gov/apis/device/recall/)
- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence)
- [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)
- [LangChain tools](https://docs.langchain.com/oss/python/langchain/tools)
- [LangChain retrieval](https://docs.langchain.com/oss/python/langchain/retrieval)
