# FDA Current-Year Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `maude refresh-fda`, which safely discovers, downloads, reconciles, validates, and atomically publishes the 12 official FDA current-year MAUDE base/add/change archives.

**Architecture:** A strict catalog adapter emits 12 typed artifact descriptions, a streaming downloader admits only validated checksum-addressed ZIPs, and a DuckDB reconciliation step selects current rows by the existing business keys. A refresh orchestrator reuses the existing snapshot quality and atomic-promotion behavior while writing separate refresh-run evidence for network and catalog phases.

**Tech Stack:** Python 3.12, Pydantic, `urllib.request`, `html.parser`, PyArrow, DuckDB, Typer, pytest, Hypothesis, Ruff, mypy

**Spec:** `docs/superpowers/specs/2026-09-07-fda-current-refresh-design.md`

## Global Constraints

- The only catalog is `https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files`.
- Every archive URL and redirect target must be HTTPS on exact host `www.accessdata.fda.gov`.
- Exactly 12 allowlisted current-year archives are required: base/add/change for master/device/narrative/patient.
- Content SHA-256, not HTTP metadata, defines source identity and refresh idempotency.
- Role precedence is `change > add > base`; absence never deletes a row.
- Existing `TableSpec.business_key` values define reconciliation identity.
- Existing blocking quality thresholds remain unchanged.
- `manifests/current.json` changes only after all download, parse, reconciliation, quality, and durability checks pass.
- No scheduler, historical ingestion, openFDA, ML, LangChain, LangGraph, API, UI, or new HTTP dependency is added.

---

### Task 1: Refresh contracts and storage paths

**Files:**
- Modify: `src/maude/domain/enums.py`
- Modify: `src/maude/domain/models.py`
- Modify: `src/maude/storage/layout.py`
- Modify: `src/maude/ingestion/manifests.py`
- Modify: `src/maude/ingestion/pipeline.py`
- Test: `tests/unit/test_layout.py`
- Test: `tests/unit/test_manifests.py`

**Interfaces:**
- Produces: `SourceRole`, `RefreshOutcome`, `CatalogEvidence`, `DiscoveredArtifact`, `DownloadedArtifact`, `ReconciliationStats`, `RefreshRunResult`, `RefreshLayout`.
- Produces: `write_json_model(path: Path, model: BaseModel) -> None` for durable strict-model persistence.
- Preserves: existing local-ingestion behavior and `TableResult.source`.

- [x] **Step 1: Write failing contract tests**

```python
def test_refresh_layout_separates_raw_objects_and_run_manifests(tmp_path: Path) -> None:
    layout = RefreshLayout(tmp_path, UUID("00000000-0000-0000-0000-000000000001"))
    assert layout.catalogs == tmp_path / "raw" / "catalogs"
    assert layout.raw_object("ab" * 32) == tmp_path / "raw" / "sha256" / "ab" / f"{'ab' * 32}.zip"
    assert layout.run_manifest == tmp_path / "refresh-runs" / "00000000-0000-0000-0000-000000000001.json"

def test_refresh_models_reject_unknown_fields() -> None:
    artifact = DiscoveredArtifact(
        table=TableKind.MASTER,
        role=SourceRole.BASE,
        filename="mdrfoi.zip",
        url="https://www.accessdata.fda.gov/MAUDE/ftparea/mdrfoi.zip",
    )
    with pytest.raises(ValidationError):
        DiscoveredArtifact.model_validate({**artifact.model_dump(), "unknown": True})

def test_table_result_records_all_contributing_sources() -> None:
    sources = tuple(
        SourceIdentity(
            path=f"/{role.value}.zip",
            filename=f"mdrfoi{'' if role is SourceRole.BASE else role.value}.txt",
            sha256=role.value,
            byte_size=1,
            archive_member=f"mdrfoi{'' if role is SourceRole.BASE else role.value}.txt",
            encoding="utf-8",
            source_role=role,
        )
        for role in SourceRole
    )
    result = table.model_copy(update={"sources": sources})
    assert [source.source_role for source in result.sources] == [
        SourceRole.BASE,
        SourceRole.ADD,
        SourceRole.CHANGE,
    ]
```

- [x] **Step 2: Run the focused tests and verify RED**

Run: `uv run pytest tests/unit/test_layout.py tests/unit/test_manifests.py -q`

Expected: collection fails because the refresh contracts and layout do not exist.

- [x] **Step 3: Implement the minimal contracts**

Add:

```python
class SourceRole(StrEnum):
    BASE = "base"
    ADD = "add"
    CHANGE = "change"

class DiscoveredArtifact(ContractModel):
    table: TableKind
    role: SourceRole
    filename: str
    url: str

class ReconciliationStats(ContractModel):
    table: TableKind
    role: SourceRole
    inserted: int = Field(ge=0)
    updated: int = Field(ge=0)
    unchanged: int = Field(ge=0)
    superseded: int = Field(ge=0)
```

Define the remaining spec fields with strict URL strings, timezone-aware datetimes, nonnegative sizes/counts, and optional HTTP headers. Define `RefreshOutcome` with `running`, `promoted`, `unchanged`, and `failed`. Add `source_role: SourceRole | None = None` to `SourceIdentity` and `sources: tuple[SourceIdentity, ...] = ()` to `TableResult`. Extend the existing `test_shared_models_validate_bounds_and_preserve_fields` test using its local `table` value, and populate `sources=(bronze.source,)` in existing local ingestion.

`RefreshLayout` creates paths only from a configured data root, validated UUID, and validated 64-character lowercase hex digest. Generalize the existing atomic JSON writer without changing snapshot-manifest serialization.

- [x] **Step 4: Run focused and regression tests**

Run: `uv run pytest tests/unit/test_layout.py tests/unit/test_manifests.py tests/integration/test_pipeline.py -q`

Expected: PASS, including existing local snapshot recovery and promotion tests.

- [x] **Step 5: Commit**

```bash
git add src/maude/domain/enums.py src/maude/domain/models.py src/maude/storage/layout.py src/maude/ingestion/manifests.py src/maude/ingestion/pipeline.py tests/unit/test_layout.py tests/unit/test_manifests.py
git commit -m "feat: add FDA refresh contracts"
```

### Task 2: Strict FDA catalog discovery

**Files:**
- Create: `src/maude/ingestion/http.py`
- Create: `src/maude/ingestion/fda_catalog.py`
- Create: `tests/unit/test_fda_catalog.py`

**Interfaces:**
- Consumes: `DiscoveredArtifact`, `SourceRole`, `TableKind`.
- Produces: `parse_current_catalog(html: str, catalog_url: str) -> tuple[DiscoveredArtifact, ...]`.
- Produces: `HttpResponse` and `UrlOpener` protocols plus `open_url(request: Request, timeout: float) -> HttpResponse`.
- Produces: `fetch_current_catalog(opener: UrlOpener, layout: RefreshLayout, retrieved_at: datetime) -> tuple[CatalogEvidence, tuple[DiscoveredArtifact, ...]]`.

- [x] **Step 1: Write failing allowlist tests**

Build one compact HTML fixture with unrelated historical/problem-code links and the 12 required links. Define `valid_catalog_html()` by joining anchors for the 12 literal filenames in the allowlist between `<h2>MAUDE Data Downloadable Files</h2>` and `<h2>Alternative Summary Reports</h2>`. Assert exact deterministic `(table, role, filename)` output. Add one focused test each for a missing artifact, duplicate artifact, HTTP URL, wrong host, unexpected port/user-info/fragment, link outside the MAUDE table section, and mismatched catalog final URL.

```python
artifacts = parse_current_catalog(valid_catalog_html(), FDA_CATALOG_URL)
assert [(item.table, item.role) for item in artifacts] == [
    (table, role) for table in TableKind for role in SourceRole
]
```

- [x] **Step 2: Run and verify RED**

Run: `uv run pytest tests/unit/test_fda_catalog.py -q`

Expected: collection fails because `maude.ingestion.fda_catalog` does not exist.

- [x] **Step 3: Implement exact discovery**

Define the shared network boundary in `http.py`:

```python
class HttpResponse(Protocol):
    headers: Message
    def read(self, amount: int = -1) -> bytes: ...
    def geturl(self) -> str: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *args: object) -> None: ...

class UrlOpener(Protocol):
    def __call__(self, request: Request, timeout: float) -> HttpResponse: ...
```

Use one `HTMLParser` subclass that records anchors only after the `MAUDE Data Downloadable Files` heading and stops at `Alternative Summary Reports`. Match URL basenames against one immutable mapping:

```python
CURRENT_ARCHIVES = {
    "mdrfoi.zip": (TableKind.MASTER, SourceRole.BASE),
    "mdrfoiadd.zip": (TableKind.MASTER, SourceRole.ADD),
    "mdrfoichange.zip": (TableKind.MASTER, SourceRole.CHANGE),
    "device.zip": (TableKind.DEVICE, SourceRole.BASE),
    "deviceadd.zip": (TableKind.DEVICE, SourceRole.ADD),
    "devicechange.zip": (TableKind.DEVICE, SourceRole.CHANGE),
    "foitext.zip": (TableKind.NARRATIVE, SourceRole.BASE),
    "foitextadd.zip": (TableKind.NARRATIVE, SourceRole.ADD),
    "foitextchange.zip": (TableKind.NARRATIVE, SourceRole.CHANGE),
    "patient.zip": (TableKind.PATIENT, SourceRole.BASE),
    "patientadd.zip": (TableKind.PATIENT, SourceRole.ADD),
    "patientchange.zip": (TableKind.PATIENT, SourceRole.CHANGE),
}
```

Validate scheme, hostname, port, user-info, and fragment after `urljoin`. Fetch through an injected opener, hash/store the exact page bytes through `RefreshLayout.catalogs`, decode from declared charset with UTF-8 fallback only when no charset is declared, and record response metadata.

- [x] **Step 4: Run focused tests**

Run: `uv run pytest tests/unit/test_fda_catalog.py -q`

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/maude/ingestion/http.py src/maude/ingestion/fda_catalog.py tests/unit/test_fda_catalog.py
git commit -m "feat: discover current FDA archives"
```

### Task 3: Immutable streaming downloader

**Files:**
- Create: `src/maude/ingestion/download.py`
- Create: `tests/unit/test_download.py`
- Modify: `src/maude/ingestion/schemas.py`

**Interfaces:**
- Consumes: `DiscoveredArtifact`, `RefreshLayout`, existing `inspect_source`, `select_source_encoding`, and `detect_table`.
- Produces: `download_artifact(artifact: DiscoveredArtifact, layout: RefreshLayout, opener: UrlOpener, started_at: datetime) -> DownloadedArtifact`.
- Produces: `detect_source_role(filename: str) -> SourceRole` for exact member-role validation.

- [ ] **Step 1: Write failing downloader tests**

Use in-memory fake HTTP responses and real tiny ZIP bytes. Prove bounded chunk reads, correct SHA-256 path, metadata capture, existing-object reuse, and cleanup after each failure. Parametrize failures for wrong redirect host, malformed/missing/negative `Content-Length`, truncated response, body above `MAX_DOWNLOAD_BYTES`, CRC failure, multiple members, unsafe member path, non-text member, wrong family, wrong role, and missing required columns.

```python
downloaded = download_artifact(artifact, layout, opener, started_at)
assert Path(downloaded.raw_path).read_bytes() == zip_bytes
assert downloaded.sha256 == hashlib.sha256(zip_bytes).hexdigest()
assert list(layout.raw_root.rglob("*.tmp")) == []
```

- [ ] **Step 2: Run and verify RED**

Run: `uv run pytest tests/unit/test_download.py -q`

Expected: collection fails because the downloader does not exist.

- [ ] **Step 3: Implement download admission**

Stream `1024 * 1024` byte blocks with `MAX_DOWNLOAD_BYTES = 2 * 1024**3`. Use `tempfile.mkstemp` beside the target, update SHA-256/count while writing, flush and sync, then inspect the temporary ZIP. Check detected table and member role against the discovered artifact before `Path.replace`. If the checksum target already exists, verify its checksum and bytesize, discard the temporary duplicate, and return the existing path.

Every exception unlinks only the invocation's temporary file. It never deletes an existing raw object.

- [ ] **Step 4: Run focused tests**

Run: `uv run pytest tests/unit/test_download.py tests/unit/test_archive.py tests/unit/test_schemas.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maude/ingestion/download.py src/maude/ingestion/schemas.py tests/unit/test_download.py
git commit -m "feat: store validated FDA downloads"
```

### Task 4: Role provenance and deterministic reconciliation

**Files:**
- Modify: `src/maude/ingestion/parser.py`
- Modify: `src/maude/ingestion/normalize.py`
- Create: `src/maude/ingestion/reconcile.py`
- Modify: `src/maude/quality/checks.py`
- Test: `tests/unit/test_parser.py`
- Create: `tests/unit/test_reconcile.py`

**Interfaces:**
- Consumes: three normalized role Parquet paths, `TableSpec.business_key`, and `SourceRole`.
- Produces: `reconcile_table(table: TableKind, role_paths: Mapping[SourceRole, Path], output_path: Path) -> tuple[ReconciliationStats, ...]`.
- Preserves: normalized FDA columns and all provenance columns on the selected row.

- [ ] **Step 1: Write failing provenance tests**

Pass `SourceIdentity(source_role=SourceRole.CHANGE, ...)` into `parse_to_bronze`; assert `_source_role == "change"` in Bronze and Silver. Assert local sources with no role remain accepted and produce a null role.

- [ ] **Step 2: Write failing reconciliation tests**

Create tiny Parquet fixtures for all four key shapes. Assert:

- change replaces a different-hash matching key;
- change/add same-hash matching keys count unchanged and choose higher-priority provenance;
- add and change insert previously unseen keys;
- absence in later roles retains the base row;
- output ordering is deterministic by business key;
- duplicate keys within one role raise `ReconciliationError`; and
- stats reconcile to input and selected counts.

- [ ] **Step 3: Run and verify RED**

Run: `uv run pytest tests/unit/test_parser.py tests/unit/test_reconcile.py -q`

Expected: provenance assertions fail and reconciliation module is missing.

- [ ] **Step 4: Implement minimal role provenance and DuckDB selection**

Add `_source_role` to `PROVENANCE_COLUMNS`, populated from `SourceIdentity.source_role`. Build a union with numeric role priority and reject duplicates using grouped business-key counts per role. Select with:

```sql
row_number() OVER (
  PARTITION BY <business-key columns>
  ORDER BY _role_priority DESC
) AS _selection_rank
```

Write only rank 1, excluding the temporary priority/rank fields. Use the existing quote helpers' escaping rules and atomic sibling replacement pattern. Calculate stats with grouped joins on business key and `record_content_hash`.

- [ ] **Step 5: Run focused and quality tests**

Run: `uv run pytest tests/unit/test_parser.py tests/unit/test_normalize.py tests/unit/test_reconcile.py tests/unit/test_quality.py -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/maude/ingestion/parser.py src/maude/ingestion/normalize.py src/maude/ingestion/reconcile.py src/maude/quality/checks.py tests/unit/test_parser.py tests/unit/test_reconcile.py
git commit -m "feat: reconcile FDA source roles"
```

### Task 5: End-to-end refresh orchestration and idempotency

**Files:**
- Create: `src/maude/ingestion/refresh.py`
- Modify: `src/maude/ingestion/pipeline.py`
- Modify: `src/maude/ingestion/manifests.py`
- Modify: `src/maude/storage/layout.py`
- Create: `tests/integration/test_refresh.py`

**Interfaces:**
- Consumes: catalog fetch, artifact download, role parsing, reconciliation, existing quality checks, and existing promotion helpers.
- Produces: `refresh_fda(settings: Settings, opener: UrlOpener | None = None, now: Callable[[], datetime] | None = None) -> RefreshRunResult`; `None` uses `datetime.now(UTC)`.
- Consumes: `RefreshOutcome` values `promoted`, `unchanged`, and `failed` from Task 1.

- [ ] **Step 1: Write failing integration tests**

Drive the orchestrator with a fake catalog/ZIP opener. Assert:

- all 12 downloads are consumed and four reconciled Parquet files promote;
- snapshot ID equals `fda-current-` plus the first 16 characters of the canonical source-set hash;
- a second identical refresh leaves Parquet mtimes and `current.json` bytes unchanged and reports `unchanged`;
- a previously known non-current fingerprint is quality-checked before republishing;
- discovery, download, parse, reconcile, quality, and promotion failures each write exact refresh-run failure evidence;
- every failure preserves pre-existing `current.json` bytes; and
- an interrupted snapshot follows the current recovery contract.

- [ ] **Step 2: Run and verify RED**

Run: `uv run pytest tests/integration/test_refresh.py -q`

Expected: collection fails because `refresh_fda` does not exist.

- [ ] **Step 3: Implement the refresh transaction**

Create one UUID run and immediately persist a running refresh-run manifest. Fetch/store the catalog, download sequentially in deterministic order, compute the fingerprint from canonical compact JSON, and derive the snapshot ID.

Before parsing, compare the 12 `(table, role, sha256)` identities with the current snapshot inputs. Return unchanged without touching snapshot/current artifacts only when they match exactly.

For changed inputs, use the existing per-snapshot advisory claim. Parse each role into distinct staging paths, reconcile into the four expected Silver paths, run current quality checks, and use the existing durable promotion state machine. Update the refresh-run manifest after terminal success or failure. Convert expected operational exceptions to a failed result; do not catch `KeyboardInterrupt`, `SystemExit`, or other `BaseException` subclasses.

- [ ] **Step 4: Run focused integration and regression tests**

Run: `uv run pytest tests/integration/test_refresh.py tests/integration/test_pipeline.py tests/integration/test_duckdb_views.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maude/ingestion/refresh.py src/maude/ingestion/pipeline.py src/maude/ingestion/manifests.py src/maude/storage/layout.py tests/integration/test_refresh.py
git commit -m "feat: orchestrate FDA current refresh"
```

### Task 6: CLI, operator documentation, and release verification

**Files:**
- Modify: `src/maude/cli.py`
- Create: `tests/integration/test_cli_refresh.py`
- Create: `docs/runbooks/fda-current-refresh.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: `refresh_fda(settings)`.
- Produces: `maude refresh-fda` with stable output and exit codes.

- [ ] **Step 1: Write failing CLI tests**

Monkeypatch only the orchestration boundary. Assert exact output prefixes and exits:

```python
assert result.exit_code == 0
assert result.stdout.startswith("PROMOTED fda-current-")

assert unchanged.exit_code == 0
assert unchanged.stdout.startswith("UNCHANGED fda-current-")

assert failed.exit_code == 1
assert failed.stdout.startswith("FAILED download ")
assert "refresh-runs/" in failed.stdout
```

- [ ] **Step 2: Run and verify RED**

Run: `uv run pytest tests/integration/test_cli_refresh.py -q`

Expected: Typer reports that command `refresh-fda` does not exist.

- [ ] **Step 3: Implement the minimal CLI and runbook**

Add one command with no URL or snapshot arguments. Print the terminal state, snapshot ID when known, catalog retrieval timestamp for success, per-family selected counts, and refresh-run manifest path for failure. Document configuration, expected disk usage, status meanings, evidence paths, safe rerun behavior, and the passive-reporting limitation. Add one README link to the runbook.

- [ ] **Step 4: Run focused CLI tests**

Run: `uv run pytest tests/integration/test_cli_refresh.py tests/integration/test_cli_audit.py -q`

Expected: PASS.

- [ ] **Step 5: Run the complete local verification gate**

Run: `make verify`

Expected: Ruff check and format pass, mypy reports no issues, and all tests pass.

- [ ] **Step 6: Run the official catalog smoke test**

Run a read-only Python invocation of `fetch_current_catalog` against the live FDA page and print only the 12 `(family, role, filename, host)` entries plus catalog hash.

Expected: exactly 12 entries, every host `www.accessdata.fda.gov`, with no duplicates or missing roles.

- [ ] **Step 7: Run one real refresh**

Run: `maude refresh-fda`

Expected: either `PROMOTED` with four published Parquet tables, or `FAILED` with exact blocking evidence and no change to the prior `current.json`. Record row counts, elapsed time, peak disk use available from the filesystem, and manifest paths in the handoff.

- [ ] **Step 8: Commit**

```bash
git add src/maude/cli.py tests/integration/test_cli_refresh.py docs/runbooks/fda-current-refresh.md README.md
git commit -m "feat: expose FDA refresh command"
```

- [ ] **Step 9: Review branch scope**

Run:

```bash
git diff --check main...HEAD
git status --short
git log --oneline main..HEAD
```

Expected: no whitespace errors, no uncommitted files, and only the approved refresh feature commits.
