# GitHub Issue Backlog and Implementation Status

These entries document the planned work and its implementation state. GitHub
currently shows zero issues for this repository and displays “Issue creation is
restricted in this repository.” No issues have been created. Once issue creation
is enabled, post the remaining items and link each merged pull request to its issue.

Effort estimates are engineering complexity, not calendar time. File paths are
repository-relative. Issues 1 and 2 have local implementations; issue 3 now has
an audit implementation but still needs review against the real snapshot and an
explicit exclusion policy. GitHub issue creation remains restricted.

## 1. Build a reproducible modeling corpus from promoted snapshots

**Effort:** High · **Roadmap:** Phase 6 — Narrative and feature engineering
**Status:** Implemented on `feature/reconciliation-conflicts`.

### Goal

Build on the implemented in-memory cleaning and report-document contracts to
produce a reproducible corpus from a pinned promoted snapshot, while retaining
original narrative rows as independently inspectable evidence. The existing
contracts are in `src/maude/features/documents.py` and
`src/maude/domain/models.py` (`ModelingNarrative`, `ModelingDocument`).

### Read first

- `docs/PROJECT_GUIDE.md`, section “Next implementation stage: narrative features”
- `docs/superpowers/specs/2026-09-04-fda-maude-signal-triage-design.md`, section 9
- `src/maude/features/documents.py` (current in-memory cleaning/document step)
- `src/maude/storage/layout.py` (managed data locations)
- `src/maude/ingestion/manifests.py` (manifest conventions)
- `src/maude/storage/duckdb.py` (`report_documents` view)
- `src/maude/domain/models.py` (`ReportDocument`, `NarrativeEvidence`, modeling contracts)
- `tests/integration/test_duckdb_views.py`

### Acceptance criteria

- A bounded-memory builder reads a pinned, promoted snapshot and writes report-level
  modeling documents outside Git.
- A versioned run manifest records snapshot identity, cleaning version, and counts
  of reports/narratives written or omitted (including blank narratives).
- Each emitted narrative retains resolvable source evidence (text key, type, file,
  source line, source hash, and snapshot provenance).
- Rerunning the builder on the same snapshot and code version produces equivalent
  records and manifest counts.
- Tests use small synthetic or appropriately de-identified fixtures only; raw FDA
  data and generated corpora are not committed.

## 2. Add a reproducible word and character TF-IDF baseline

**Effort:** High · **Roadmap:** Phase 6 — Narrative and feature engineering
**Status:** Implemented on `feature/reconciliation-conflicts`.

### Goal

Produce versioned word and character TF-IDF matrices and feature metadata from the
clean report documents, without fitting vocabulary on held-out time periods.

### Read first

- `docs/superpowers/specs/2026-09-04-fda-maude-signal-triage-design.md`, sections
  9 and 10
- `src/maude/storage/duckdb.py`
- `src/maude/domain/models.py`
- `pyproject.toml` and `uv.lock`
- Issue 1 in this backlog

### Acceptance criteria

- Training, validation, and test boundaries are time-based and fixed before fitting.
- Word and character n-gram baselines are separately reproducible.
- Vectorizer parameters, vocabulary/model artifacts, dataset snapshot, feature
  version, and environment lock identity are recorded.
- Feature generation is bounded-memory: vocabularies fit from a deterministic,
  capped training-only sample, then transform all periods in sparse matrix batches.
- A small fixture proves deterministic output and prevents vocabulary leakage.
- No raw corpus or generated model/vector files are committed.

**Implementation:** `src/maude/features/tfidf.py`; CLI: `maude build-tfidf`.
The shared split boundaries live in `src/maude/features/splits.py`. Review the
duplicate audit from issue 3 before proceeding to clustering.

## 3. Build temporal train/validation/test splits and leakage checks

**Effort:** High · **Roadmap:** Phase 7 — Clustering and statistical evaluation
**Status:** Audit implementation complete and fixture-tested; full-snapshot audit
has not completed. Split-exclusion policy and historical bootstrap remain open.

### Goal

Define reproducible time-based evaluation splits that prevent future reports,
duplicate narratives, and repeated source records from leaking into training.

### Read first

- Design spec, sections 9, 10, and 11
- `src/maude/ingestion/reconcile.py`
- `src/maude/storage/duckdb.py`
- `tests/integration/test_duckdb_views.py`
- Issues 1 and 2 in this backlog

### Acceptance criteria

- Split definitions are versioned and tied to a dataset snapshot.
- Duplicate groups and repeated business keys cannot cross split boundaries.
- Any cohort or date exclusions are reported with counts and reasons.
- Tests prove the split is deterministic and leakage checks detect injected overlap.
- An evaluation manifest records date windows, exclusions, and source snapshot.

**Implemented slice:** `src/maude/features/splits.py` defines the shared, versioned
2024/2025/2026+ boundaries. `src/maude/features/duplicates.py` adds a disk-backed
audit of exact narrative groups, repeated report IDs across periods, and approximate
near-duplicate pairs. Missing-date narratives are counted, results are checksum-
bound, and no source records are modified or silently excluded. Near-duplicate
candidate generation is approximate and reports its strategy/threshold; it is not
an exhaustive proof that no near duplicate exists. A preliminary date-split count
on `fda-current-43bedbf20c68c84b` found 30,279 training narratives, 34,430
validation narratives, and 4,178,516 test narratives (2026-01-01 through
2026-08-31). The highly skewed cohort requires historical bootstrap before model
evaluation can be interpreted; the complete duplicate audit and findings review
must also precede clustering. Two full audit attempts were stopped after about
38 and 45 CPU minutes without publishing partial output; no full-snapshot
duplicate counts are available. Add progress/resume support or optimize runtime,
then rerun the immutable snapshot audit and review the resulting manifest before
clustering.

## 4. Implement and evaluate failure-pattern clustering

**Effort:** XHigh · **Roadmap:** Phase 7 — Clustering and statistical evaluation

### Goal

Benchmark scalable narrative clustering, beginning with MiniBatchKMeans, and select
configurations using reproducibility, stability, coherence, and researcher review.

### Read first

- Design spec, sections 9 and 10
- Issues 1–3 in this backlog
- `pyproject.toml`, `uv.lock`, `Makefile`

### Acceptance criteria

- The TF-IDF baseline is compared with any dimensionality reduction before adopting
  the reduction.
- Cohort training uses product code/device family where counts permit and documents
  a sparse-cohort fallback.
- At least five seeds and bootstrap samples are evaluated.
- Reports include cluster sizes, outlier coverage, top terms, temporal stability,
  duplicate sensitivity, and representative reports for review.
- Every assignment has model, feature, and dataset snapshot versions.
- Machine-generated labels are visibly unreviewed; no automatic medical
  interpretation is emitted.

## 5. Add transparent signal indicators and backtests

**Effort:** XHigh · **Roadmap:** Phase 7 — Clustering and statistical evaluation

### Goal

Compute inspectable reported-severity, novelty, cluster-confidence, evidence-density,
and report-volume-change indicators, then evaluate their behavior on historical
time windows.

### Read first

- Design spec, sections 2, 10, and 12
- Issues 1–4 in this backlog
- `src/maude/quality/checks.py`
- `src/maude/domain/models.py`

### Acceptance criteria

- Indicators preserve unknown and missing values and publish formula/version
  metadata.
- Volume change is described as report-volume change, never incidence or risk.
- Temporal backtests compare the transparent baseline with alternatives.
- Evaluation includes false-positive review burden, threshold sensitivity, and
  synthetic emerging-pattern tests.
- Output is tied to a source snapshot, feature version, and policy version.

## 6. Build source-linked lexical and semantic retrieval

**Effort:** High · **Roadmap:** Phase 8 — Evidence retrieval and LangGraph triage

### Goal

Retrieve comparable historical reports with exact evidence identifiers and evaluate
lexical, semantic, and hybrid approaches.

### Read first

- Design spec, sections 11 and 13
- Issues 1–5 in this backlog
- `src/maude/storage/duckdb.py`
- `src/maude/domain/models.py`

### Acceptance criteria

- Returned evidence resolves to its source text and snapshot 100% of the time.
- Retrieval includes explicit cohort/date/manufacturer filter reasons and duplicate
  diversity metadata.
- Lexical-only performance is measured before semantic retrieval is introduced.
- A labeled benchmark reports recall@5/10, MRR, filter accuracy, latency, and
  citation integrity.
- FAISS is used for local reproducible benchmarking; production storage choice is
  documented separately.

## 7. Add typed LangChain research tools and a durable LangGraph workflow

**Effort:** High · **Roadmap:** Phase 8 — Evidence retrieval and LangGraph triage

### Goal

Orchestrate validated signal facts and evidence into resumable triage cases with
explicit human review states.

### Read first

- Design spec, sections 10–13
- Issues 5 and 6 in this backlog
- `src/maude/domain/models.py`
- `src/maude/config.py`

### Acceptance criteria

- LangChain tools have typed inputs/outputs and deterministic behavior in the
  baseline workflow.
- LangGraph state stores IDs/version references rather than DataFrames or model
  objects.
- Routine, investigate, quarantine, failure-retry, and human-interrupt paths are
  replayable and auditable.
- A failed evidence tool cannot silently lower a case to routine.
- LLM summaries are out of scope for this issue; evidence packets work without an
  LLM.

## 8. Persist human review decisions and case history

**Effort:** High · **Roadmap:** Phase 8 — Evidence retrieval and LangGraph triage

### Goal

Persist reviewer decisions, notes, timestamps, reason codes, and evidence viewed so
triage remains accountable across graph resumes.

### Read first

- Design spec, sections 12 and 14
- Issue 7 in this backlog
- `src/maude/domain/models.py`

### Acceptance criteria

- Every decision references case, source snapshot, model/feature/policy versions,
  reviewer, and evidence packet.
- Review history is append-only or otherwise auditable.
- Public read paths cannot mutate review state.
- Tests cover resume, duplicate submissions, authorization boundaries, and history
  reconstruction.

## 9. Add a read-only FastAPI research API

**Effort:** High · **Roadmap:** Phase 9 — Public research product

### Goal

Serve versioned reports, devices, clusters, signals, snapshots, evidence, and
methodology through bounded read-only endpoints.

### Read first

- Design spec, sections 14 and 16
- Issues 4–8 in this backlog
- `src/maude/domain/models.py`
- `src/maude/config.py`

### Acceptance criteria

- Responses include provenance and applicable dataset/feature/model versions.
- Search filters and pagination are bounded and validated.
- Public endpoints cannot change ingestion, model, or review state.
- OpenAPI schemas, fixture-backed endpoint tests, and API version policy exist.
- Limitations and source evidence are represented in response contracts.

## 10. Build the public research interface and Tableau extracts

**Effort:** High · **Roadmap:** Phase 9 — Public research product

### Goal

Expose current signals, device/product-code trends, clusters, report evidence,
methodology, and validated Tableau-ready extracts.

### Read first

- Design spec, sections 14 and 15
- Issues 4–9 in this backlog
- API contract from Issue 9

### Acceptance criteria

- Public pages show snapshot date, versions, source citations, and limitations.
- Charts have accessible labels and an equivalent tabular representation.
- Tableau extracts consume validated gold outputs and do not recalculate metrics.
- UI tests cover search, evidence inspection, empty results, and stale-data status.

## 11. Schedule refreshes and monitor data/model health

**Effort:** High · **Roadmap:** Phase 10 — Production operations and release

### Goal

Run the FDA catalog check on a schedule, refresh changed source content safely, and
alert operators to failures, staleness, or data/model drift.

### Read first

- `docs/runbooks/fda-current-refresh.md`
- `docs/superpowers/specs/2026-09-07-fda-current-refresh-design.md`
- `src/maude/ingestion/refresh.py`
- `src/maude/storage/layout.py`
- `src/maude/domain/models.py`

### Acceptance criteria

- Scheduler is separate from LangGraph and invokes the existing refresh contract.
- Concurrent runs are controlled; retries preserve manifests and last-known-good
  pointer.
- Monitoring records last success, source freshness, row counts, quality warnings,
  storage usage, and task duration.
- An operator can recover using documented steps and a restore drill.

## 12. Prepare the public release and governance package

**Effort:** Medium · **Roadmap:** Phase 10 — Production operations and release

### Goal

Publish complete methods, data dictionary, model cards, appropriate-use language,
security/privacy review, and versioned release procedures.

### Read first

- `README.md`
- `docs/PROJECT_GUIDE.md`
- Design spec sections 2, 3, 13, and 14
- `pyproject.toml`, `uv.lock`, and deployment files as they are added

### Acceptance criteria

- Public documentation explains passive-reporting limits, source coverage, rejected
  data, warnings, and known biases.
- Every dataset, feature, model, and triage policy release can be reconstructed from
  versioned manifests.
- Licensing, privacy, dependency, security, accessibility, and incident-response
  reviews are documented.
- Release instructions exclude raw data, local caches, credentials, and unreviewed
  model artifacts.
