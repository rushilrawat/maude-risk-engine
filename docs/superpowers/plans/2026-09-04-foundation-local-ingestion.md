# MAUDE Foundation and Local Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a reproducible Python foundation that validates the existing FDA archives, streams the master/device/patient/narrative tables into versioned Parquet, rejects malformed records without silent truncation, and exposes a DuckDB report-document view with source provenance.

**Architecture:** The first vertical slice reads source archives directly without loading whole files into memory, records immutable source and run manifests, writes raw-string bronze Parquet, normalizes selected canonical fields into silver Parquet, and runs blocking quality checks before promoting a snapshot. This plan deliberately stops before clustering and LangGraph; those consumers will rely on the stable `SnapshotResult` and canonical report-document contracts produced here.

**Tech Stack:** Python 3.12+, uv, Pydantic 2, Typer, PyArrow, DuckDB, pytest, Hypothesis, Ruff, mypy

**Spec:** `docs/superpowers/specs/2026-09-04-fda-maude-signal-triage-design.md`

## Global Constraints

- FDA MAUDE bulk files are canonical; local hand-converted UTF-8 files are untrusted inputs.
- Raw archives and generated data must never be committed to Git.
- Processing must be bounded-memory and must not call `pandas.read_csv` on whole source files.
- Every accepted and rejected row must be attributable to a source snapshot and parser version.
- Failed quality gates must leave the last promoted snapshot unchanged.
- Preserve raw strings while exposing separately parsed dates and normalized null values.
- Never describe MAUDE output as incidence, causality, or comparative device risk.
- Python code targets Python 3.12 or newer and passes Ruff, mypy, and pytest.

---

## Plan Boundary

This is implementation plan 1 of 5:

1. Foundation and local ingestion — this document.
2. Automated FDA discovery, historical bootstrap, and incremental refresh.
3. Narrative features, clustering, signal indicators, and time-split evaluation.
4. Hybrid retrieval, LangGraph triage, and human review.
5. FastAPI, public/reviewer web application, Tableau, and deployment.

The plans are sequential because downstream model, graph, and product contracts depend on verified source identities and canonical tables.

## Target File Map

```text
maude-risk-engine/
├── .env.example                         # portable local configuration example
├── .gitignore                           # prevents raw/generated data commits
├── Makefile                             # stable developer commands
├── pyproject.toml                       # package, dependencies, lint/test config
├── README.md                            # setup and first vertical-slice commands
├── src/maude/
│   ├── __init__.py
│   ├── cli.py                           # Typer entry point
│   ├── config.py                        # environment-backed paths and batch size
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── enums.py                     # table/run/quality enums
│   │   └── models.py                    # manifests and snapshot result contracts
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── archive.py                   # archive identity, hashing, safe member access
│   │   ├── encoding.py                  # strict encoding selection
│   │   ├── schemas.py                   # table detection, required columns, keys, dates
│   │   ├── parser.py                    # bounded-memory pipe parser to bronze Parquet
│   │   ├── normalize.py                 # bronze-to-silver canonical transformations
│   │   ├── manifests.py                 # atomic JSON manifest persistence
│   │   └── pipeline.py                  # stage orchestration and atomic promotion
│   ├── quality/
│   │   ├── __init__.py
│   │   ├── checks.py                    # blocking/warning check implementations
│   │   └── report.py                    # JSON and Markdown quality report renderer
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── layout.py                    # deterministic snapshot paths
│   │   └── duckdb.py                    # canonical views and report documents
│   └── service/
│       ├── __init__.py
│       └── audit.py                     # local archive audit use case
├── tests/
│   ├── conftest.py
│   ├── fixtures/
│   │   └── README.md                    # fixture privacy/provenance policy
│   ├── unit/
│   │   ├── test_config.py
│   │   ├── test_archive.py
│   │   ├── test_encoding.py
│   │   ├── test_schemas.py
│   │   ├── test_parser.py
│   │   ├── test_normalize.py
│   │   ├── test_quality.py
│   │   └── test_layout.py
│   └── integration/
│       ├── test_pipeline.py
│       └── test_duckdb_views.py
└── docs/runbooks/local-ingestion.md
```

## Shared Interfaces

The following contracts must remain consistent across tasks:

```python
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class TableKind(StrEnum):
    MASTER = "master"
    DEVICE = "device"
    PATIENT = "patient"
    NARRATIVE = "narrative"


class RunStatus(StrEnum):
    RUNNING = "running"
    FAILED = "failed"
    VALIDATED = "validated"
    PROMOTED = "promoted"


class QualityLevel(StrEnum):
    BLOCKING = "blocking"
    WARNING = "warning"


class SourceIdentity(BaseModel):
    path: str
    filename: str
    sha256: str
    byte_size: int = Field(ge=0)
    archive_member: str | None
    encoding: str


class ParseStats(BaseModel):
    rows_seen: int = Field(ge=0)
    rows_accepted: int = Field(ge=0)
    rows_rejected: int = Field(ge=0)
    columns: tuple[str, ...]


class QualityResult(BaseModel):
    check: str
    level: QualityLevel
    passed: bool
    message: str
    metrics: Mapping[str, int | float | str | None] = Field(default_factory=dict)


class TableResult(BaseModel):
    table: TableKind
    source: SourceIdentity
    bronze_path: str
    silver_path: str
    reject_path: str
    stats: ParseStats
    quality: Sequence[QualityResult]


class SnapshotResult(BaseModel):
    run_id: UUID
    snapshot_id: str
    started_at: datetime
    finished_at: datetime | None
    status: RunStatus
    tables: Sequence[TableResult]
    quality: Sequence[QualityResult]
    promoted_path: str | None
    parser_version: str


@dataclass(frozen=True)
class InspectedSource:
    path: Path
    member: str | None
    sha256: str
    byte_size: int
    sample: bytes


class BronzeResult(BaseModel):
    table: TableKind
    source: SourceIdentity
    bronze_path: str
    reject_path: str
    stats: ParseStats


class NormalizationResult(BaseModel):
    silver_path: str
    date_parse_failure_count: int = Field(ge=0)


class LocalAuditItem(BaseModel):
    table: TableKind
    archive_path: str
    archive_member: str
    sha256: str
    encoding: str
    converted_path: str | None
    conversion_row_ratio: float | None
    quality: Sequence[QualityResult]


class LocalAuditResult(BaseModel):
    source_root: str
    items: Sequence[LocalAuditItem]
    quality: Sequence[QualityResult]
    has_blocking_failure: bool
```

---

### Task 1: Bootstrap the Python Package and Safe Repository Defaults

**Files:**
- Create: `pyproject.toml`
- Create: `.env.example`
- Modify: `.gitignore`
- Create: `Makefile`
- Create: `src/maude/__init__.py`
- Create: `src/maude/cli.py`
- Create: `tests/test_smoke.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: no application interfaces.
- Produces: installable `maude` package and `maude` CLI entry point.

- [ ] **Step 1: Extend data and environment exclusions before adding code**

Ensure `.gitignore` contains these exact categories in addition to existing entries:

```gitignore
data/raw/
data/bronze/
data/silver/
data/gold/
data/staging/
data/rejects/
artifacts/
models/
*.zip
*.parquet
*.duckdb
*.faiss
*.index
.env
.venv/
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
.DS_Store
```

Run: `git check-ignore data/raw/example.zip data/silver/example.parquet .env`  
Expected: all three paths are printed.

- [ ] **Step 2: Write the failing package smoke test**

```python
from typer.testing import CliRunner

from maude import __version__
from maude.cli import app


def test_package_exposes_version_and_cli() -> None:
    assert __version__ == "0.1.0"
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == "0.1.0"
```

- [ ] **Step 3: Add the package and tool configuration**

Create `pyproject.toml` with:

```toml
[build-system]
requires = ["hatchling>=1.27"]
build-backend = "hatchling.build"

[project]
name = "maude-risk-engine"
version = "0.1.0"
description = "Reproducible FDA MAUDE reporting-signal analysis and triage"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
  "duckdb>=1.3,<2",
  "pyarrow>=19,<22",
  "pydantic>=2.10,<3",
  "pydantic-settings>=2.7,<3",
  "typer>=0.15,<1",
]

[project.optional-dependencies]
dev = [
  "hypothesis>=6.120,<7",
  "mypy>=1.14,<2",
  "pytest>=8.3,<9",
  "pytest-cov>=6,<8",
  "ruff>=0.9,<1",
]

[project.scripts]
maude = "maude.cli:app"

[tool.hatch.build.targets.wheel]
packages = ["src/maude"]

[tool.pytest.ini_options]
addopts = "--strict-config --strict-markers -q"
testpaths = ["tests"]

[tool.ruff]
target-version = "py312"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "SIM", "RUF"]

[tool.mypy]
python_version = "3.12"
strict = true
packages = ["maude"]
```

Create `src/maude/__init__.py`:

```python
__version__ = "0.1.0"
```

Create `src/maude/cli.py`:

```python
import typer

from maude import __version__

app = typer.Typer(no_args_is_help=True)


@app.command()
def version() -> None:
    """Print the installed application version."""
    typer.echo(__version__)
```

- [ ] **Step 4: Add stable developer commands and portable configuration example**

Create `.env.example`:

```dotenv
MAUDE_DATA_ROOT=/absolute/path/to/fda-maude-data
MAUDE_BATCH_ROWS=50000
```

Create `Makefile`:

```makefile
.PHONY: install test lint typecheck verify

install:
	uv sync --all-extras

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy

verify: lint typecheck test
```

Update `README.md` so its first paragraph calls the project a reporting-signal and triage system, not a clinical risk estimator. Include `uv sync --all-extras`, copying `.env.example` to `.env`, and `make verify` as the setup flow.

- [ ] **Step 5: Install and run the smoke test**

Run: `uv sync --all-extras && uv run pytest tests/test_smoke.py -v`  
Expected: `test_package_exposes_version_and_cli` passes and `uv.lock` is created.

- [ ] **Step 6: Run static checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy`  
Expected: all commands exit zero.

- [ ] **Step 7: Commit the foundation**

```bash
git add .gitignore .env.example Makefile README.md pyproject.toml uv.lock src tests/test_smoke.py
git commit -m "build: initialize MAUDE Python project"
```

---

### Task 2: Define Configuration, Domain Models, and Snapshot Layout

**Files:**
- Create: `src/maude/config.py`
- Create: `src/maude/domain/__init__.py`
- Create: `src/maude/domain/enums.py`
- Create: `src/maude/domain/models.py`
- Create: `src/maude/storage/__init__.py`
- Create: `src/maude/storage/layout.py`
- Create: `tests/unit/test_config.py`
- Create: `tests/unit/test_layout.py`

**Interfaces:**
- Consumes: Pydantic configuration from Task 1.
- Produces: `Settings`, `SnapshotLayout`, and all shared Pydantic contracts shown above.

- [ ] **Step 1: Write failing configuration and layout tests**

```python
from pathlib import Path

import pytest
from pydantic import ValidationError

from maude.config import Settings
from maude.storage.layout import SnapshotLayout


def test_settings_reject_batch_size_below_one(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        Settings(data_root=tmp_path, batch_rows=0)


def test_snapshot_layout_keeps_staging_separate_from_promoted(tmp_path: Path) -> None:
    layout = SnapshotLayout(data_root=tmp_path, snapshot_id="2025-06-local")
    assert layout.staging == tmp_path / "staging" / "2025-06-local"
    assert layout.promoted == tmp_path / "silver" / "2025-06-local"
    assert layout.manifest == tmp_path / "manifests" / "2025-06-local.json"
    assert layout.staging != layout.promoted
```

- [ ] **Step 2: Implement environment-backed settings**

```python
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MAUDE_", env_file=".env")

    data_root: Path
    batch_rows: int = Field(default=50_000, ge=1, le=500_000)
    parser_version: str = "1.0.0"
```

- [ ] **Step 3: Implement shared enums and Pydantic models**

Put `TableKind`, `RunStatus`, and `QualityLevel` in `domain/enums.py`. Put `SourceIdentity`, `ParseStats`, `QualityResult`, `TableResult`, and `SnapshotResult` in `domain/models.py` using the exact fields and types in Shared Interfaces. Add `datetime` validators requiring timezone-aware timestamps.

- [ ] **Step 4: Implement deterministic snapshot paths**

```python
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SnapshotLayout:
    data_root: Path
    snapshot_id: str

    @property
    def staging(self) -> Path:
        return self.data_root / "staging" / self.snapshot_id

    @property
    def promoted(self) -> Path:
        return self.data_root / "silver" / self.snapshot_id

    @property
    def bronze(self) -> Path:
        return self.staging / "bronze"

    @property
    def silver(self) -> Path:
        return self.staging / "silver"

    @property
    def rejects(self) -> Path:
        return self.staging / "rejects"

    @property
    def manifest(self) -> Path:
        return self.data_root / "manifests" / f"{self.snapshot_id}.json"

    @property
    def current_manifest(self) -> Path:
        return self.data_root / "manifests" / "current.json"
```

Use this exact validator on `SnapshotResult` so persisted run timestamps are never naive:

```python
from pydantic import field_validator


@field_validator("started_at", "finished_at")
@classmethod
def require_timezone(cls, value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        raise ValueError("run timestamps must be timezone-aware")
    return value
```

- [ ] **Step 5: Run tests and checks**

Run: `uv run pytest tests/unit/test_config.py tests/unit/test_layout.py -v && make lint && make typecheck`  
Expected: all tests and static checks pass.

- [ ] **Step 6: Commit configuration and contracts**

```bash
git add src/maude/config.py src/maude/domain src/maude/storage tests/unit
git commit -m "feat: define ingestion contracts and snapshot layout"
```

---

### Task 3: Identify Tables and Enforce Source Schemas

**Files:**
- Create: `src/maude/ingestion/__init__.py`
- Create: `src/maude/ingestion/schemas.py`
- Create: `tests/unit/test_schemas.py`

**Interfaces:**
- Consumes: `TableKind` from Task 2.
- Produces: `TableSpec`, `TABLE_SPECS`, `detect_table(filename, columns)`, `spec_for(kind)`, `normalize_column(name)`.

- [ ] **Step 1: Write failing schema tests**

```python
import pytest

from maude.domain.enums import TableKind
from maude.ingestion.schemas import SchemaMismatch, detect_table, normalize_column


def test_normalize_column_is_stable() -> None:
    assert normalize_column("UDI-DI") == "udi_di"
    assert normalize_column("MANUFACTURER_LINK_FLAG_") == "manufacturer_link_flag"


@pytest.mark.parametrize(
    ("filename", "columns", "expected"),
    [
        ("mdrfoi.txt", ("MDR_REPORT_KEY", "EVENT_TYPE"), TableKind.MASTER),
        ("DEVICE.txt", ("MDR_REPORT_KEY", "DEVICE_SEQUENCE_NO"), TableKind.DEVICE),
        ("patient.txt", ("MDR_REPORT_KEY", "PATIENT_SEQUENCE_NUMBER"), TableKind.PATIENT),
        ("foitext.txt", ("MDR_REPORT_KEY", "MDR_TEXT_KEY", "FOI_TEXT"), TableKind.NARRATIVE),
    ],
)
def test_detect_table(filename: str, columns: tuple[str, ...], expected: TableKind) -> None:
    assert detect_table(filename, columns).kind is expected


def test_detect_table_rejects_missing_required_column() -> None:
    with pytest.raises(SchemaMismatch, match="FOI_TEXT"):
        detect_table("foitext.txt", ("MDR_REPORT_KEY", "MDR_TEXT_KEY"))
```

- [ ] **Step 2: Implement table specifications**

```python
from dataclasses import dataclass
import re

from maude.domain.enums import TableKind


class SchemaMismatch(ValueError):
    pass


@dataclass(frozen=True)
class TableSpec:
    kind: TableKind
    filename_tokens: tuple[str, ...]
    required_columns: tuple[str, ...]
    business_key: tuple[str, ...]
    date_columns: tuple[str, ...]


TABLE_SPECS = (
    TableSpec(TableKind.MASTER, ("mdrfoi",), ("MDR_REPORT_KEY", "DATE_RECEIVED", "EVENT_TYPE"), ("MDR_REPORT_KEY",), ("DATE_RECEIVED", "DATE_REPORT", "DATE_OF_EVENT")),
    TableSpec(TableKind.DEVICE, ("device", "foidev"), ("MDR_REPORT_KEY", "DEVICE_SEQUENCE_NO", "DEVICE_REPORT_PRODUCT_CODE"), ("MDR_REPORT_KEY", "DEVICE_SEQUENCE_NO"), ("DATE_RECEIVED", "DATE_RETURNED_TO_MANUFACTURER")),
    TableSpec(TableKind.PATIENT, ("patient",), ("MDR_REPORT_KEY", "PATIENT_SEQUENCE_NUMBER"), ("MDR_REPORT_KEY", "PATIENT_SEQUENCE_NUMBER"), ("DATE_RECEIVED",)),
    TableSpec(TableKind.NARRATIVE, ("foitext",), ("MDR_REPORT_KEY", "MDR_TEXT_KEY", "TEXT_TYPE_CODE", "FOI_TEXT"), ("MDR_TEXT_KEY",), ("DATE_REPORT",)),
)


def normalize_column(name: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    return re.sub(r"_+", "_", value)
```

Implement `detect_table` by matching the lowercase filename token, then computing and reporting every missing required column. Reject ambiguous or unmatched files rather than guessing.

Implement `spec_for(kind: TableKind) -> TableSpec` by requiring exactly one entry in `TABLE_SPECS` with the requested kind and returning it. A duplicate or absent kind raises `SchemaMismatch` because it is a programmer/configuration error.

- [ ] **Step 3: Run focused tests**

Run: `uv run pytest tests/unit/test_schemas.py -v`  
Expected: all parameter cases pass and the missing `FOI_TEXT` message is explicit.

- [ ] **Step 4: Commit schema detection**

```bash
git add src/maude/ingestion tests/unit/test_schemas.py
git commit -m "feat: validate MAUDE table schemas"
```

---

### Task 4: Validate Archives, Compute Identity, and Select Encoding

**Files:**
- Create: `src/maude/ingestion/archive.py`
- Create: `src/maude/ingestion/encoding.py`
- Create: `tests/unit/test_archive.py`
- Create: `tests/unit/test_encoding.py`

**Interfaces:**
- Consumes: `SourceIdentity` from Task 2.
- Produces: `sha256_file(path)`, `inspect_source(path)`, `open_source_text(path, member=None)`, `select_encoding(sample)`.

- [ ] **Step 1: Write archive safety tests**

```python
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from maude.ingestion.archive import ArchiveValidationError, inspect_source


def test_rejects_archive_path_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "bad.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr("../patient.txt", "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\r\n1|1\r\n")
    with pytest.raises(ArchiveValidationError, match="unsafe member"):
        inspect_source(archive)


def test_inspects_single_text_member(tmp_path: Path) -> None:
    archive = tmp_path / "patient.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr("patient.txt", "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\r\n1|1\r\n")
    inspected = inspect_source(archive)
    assert inspected.member == "patient.txt"
    assert len(inspected.sha256) == 64
    assert inspected.byte_size == archive.stat().st_size
```

- [ ] **Step 2: Write strict encoding tests**

```python
import pytest

from maude.ingestion.encoding import EncodingError, select_encoding


def test_prefers_utf8_when_sample_is_valid() -> None:
    assert select_encoding("café".encode()) == "utf-8"


def test_uses_cp1252_for_legacy_fda_text() -> None:
    assert select_encoding(b"device\x92s label") == "cp1252"


def test_rejects_binary_nulls() -> None:
    with pytest.raises(EncodingError, match="NUL"):
        select_encoding(b"a\x00b")
```

- [ ] **Step 3: Implement hashing and archive validation**

`inspect_source` must:

1. hash the original file in 1 MiB chunks;
2. call `ZipFile.testzip()` for ZIP input;
3. reject absolute member names and any member containing `..` path components;
4. accept exactly one non-directory `.txt` member for this vertical slice;
5. read a 64 KiB sample without extracting the member; and
6. return an immutable `InspectedSource(path, member, sha256, byte_size, sample)` dataclass.

Implement hashing as:

```python
def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
```

- [ ] **Step 4: Implement strict text selection and streaming access**

`select_encoding` must reject NUL bytes, try `utf-8` with strict decoding, then `cp1252` with strict decoding. `open_source_text` returns a context manager wrapping the raw file or ZIP member with `io.TextIOWrapper(..., encoding=encoding, errors="strict", newline="")`. It must never materialize the full archive member.

- [ ] **Step 5: Run tests and static checks**

Run: `uv run pytest tests/unit/test_archive.py tests/unit/test_encoding.py -v && make lint && make typecheck`  
Expected: all checks pass.

- [ ] **Step 6: Commit archive and encoding validation**

```bash
git add src/maude/ingestion/archive.py src/maude/ingestion/encoding.py tests/unit
git commit -m "feat: validate and stream FDA source archives"
```

---

### Task 5: Stream Pipe-Delimited Records into Bronze Parquet

**Files:**
- Create: `src/maude/ingestion/parser.py`
- Create: `tests/unit/test_parser.py`

**Interfaces:**
- Consumes: `TableSpec`, inspected source, selected encoding, `batch_rows`.
- Produces: `parse_to_bronze(source_path, target_path, reject_path, batch_rows) -> BronzeResult` where `BronzeResult` contains `table`, `source`, and `ParseStats`.

- [ ] **Step 1: Write failing accepted/rejected row test**

```python
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pyarrow.parquet as pq

from maude.ingestion.parser import parse_to_bronze


def test_parser_streams_valid_rows_and_records_bad_field_count(tmp_path: Path) -> None:
    source = tmp_path / "foitext.zip"
    content = (
        "MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|DATE_REPORT|FOI_TEXT\r\n"
        "1|10|N|01/02/2025|Pump stopped unexpectedly\r\n"
        "2|11|N|01/03/2025\r\n"
    )
    with ZipFile(source, "w", ZIP_DEFLATED) as handle:
        handle.writestr("foitext.txt", content)

    result = parse_to_bronze(
        source,
        tmp_path / "bronze.parquet",
        tmp_path / "rejects.jsonl",
        batch_rows=1,
    )

    assert result.stats.rows_seen == 2
    assert result.stats.rows_accepted == 1
    assert result.stats.rows_rejected == 1
    assert pq.read_table(tmp_path / "bronze.parquet").to_pylist()[0]["MDR_REPORT_KEY"] == "1"
    assert '"reason":"field_count"' in (tmp_path / "rejects.jsonl").read_text()
```

- [ ] **Step 2: Write the bounded-batch test**

Use a spy writer injected through the parser’s private `_write_batch` seam and assert that 5 valid rows with `batch_rows=2` produce batch lengths `[2, 2, 1]`. Do not assert process memory from a flaky platform-dependent measurement.

- [ ] **Step 3: Implement streaming parsing**

The parser must:

- read the header first and call `detect_table`;
- retain original FDA headers in bronze output;
- use `csv.reader(delimiter="|", quoting=csv.QUOTE_NONE)`;
- add `_source_line_number`, `_source_snapshot_sha256`, and `_source_filename` columns;
- write `batch_rows` at a time with a single `pyarrow.parquet.ParquetWriter`;
- encode all FDA fields as nullable strings;
- write rejected rows as compact JSON Lines with line number, reason, expected/actual field counts, and the raw row truncated to 2,000 characters; and
- reject a row with reason `missing_business_key` when any `TableSpec.business_key` field is blank;
- atomically replace the final Parquet/reject paths only after successful close.

Do not catch `UnicodeDecodeError`, archive corruption, or write failure as row rejection. Those errors fail the entire source.

- [ ] **Step 4: Add conservation property test**

With Hypothesis, generate lists of valid and one-field-short patient rows and assert:

```python
result.stats.rows_seen == result.stats.rows_accepted + result.stats.rows_rejected
```

Also assert that every accepted row appears exactly once in Parquet.

- [ ] **Step 5: Run parser tests**

Run: `uv run pytest tests/unit/test_parser.py -v`  
Expected: accepted/rejected, bounded batch, and conservation tests pass.

- [ ] **Step 6: Commit streaming parser**

```bash
git add src/maude/ingestion/parser.py tests/unit/test_parser.py
git commit -m "feat: stream MAUDE records to bronze parquet"
```

---

### Task 6: Normalize Bronze Tables into Canonical Silver Parquet

**Files:**
- Create: `src/maude/ingestion/normalize.py`
- Create: `tests/unit/test_normalize.py`

**Interfaces:**
- Consumes: bronze Parquet plus `TableSpec`.
- Produces: `normalize_bronze(bronze_path, silver_path, spec, snapshot_id) -> NormalizationResult`.

- [ ] **Step 1: Write failing normalization test**

```python
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from maude.domain.enums import TableKind
from maude.ingestion.normalize import normalize_bronze
from maude.ingestion.schemas import spec_for


def test_normalize_master_preserves_raw_date_and_parses_typed_date(tmp_path: Path) -> None:
    bronze = tmp_path / "master.parquet"
    pq.write_table(
        pa.table(
            {
                "MDR_REPORT_KEY": ["1", "2"],
                "DATE_RECEIVED": ["01/02/2025", ""],
                "EVENT_TYPE": ["D", "NI"],
                "_source_line_number": [2, 3],
                "_source_snapshot_sha256": ["abc", "abc"],
                "_source_filename": ["mdrfoi.txt", "mdrfoi.txt"],
            }
        ),
        bronze,
    )

    result = normalize_bronze(bronze, tmp_path / "silver.parquet", spec_for(TableKind.MASTER), "local-2025-06")
    rows = pq.read_table(result.silver_path).to_pylist()
    assert rows[0]["date_received_raw"] == "01/02/2025"
    assert rows[0]["date_received"] == date(2025, 1, 2)
    assert rows[1]["date_received"] is None
    assert rows[0]["dataset_snapshot_id"] == "local-2025-06"
    assert result.date_parse_failure_count == 0
```

- [ ] **Step 2: Implement canonical naming and null normalization**

Use DuckDB to transform Parquet without loading a full table into Python. Normalize all headers with `normalize_column`. For each declared date column, retain `<name>_raw` and create `<name>` with:

```sql
try_strptime(nullif(trim("DATE_RECEIVED"), ''), ['%m/%d/%Y', '%Y/%m/%d'])::DATE
```

Normalize only empty strings to SQL `NULL`; preserve FDA sentinels such as `NI`, `NA`, `UNK`, and `*` as explicit strings. Add `dataset_snapshot_id` and a deterministic `record_content_hash` computed from normalized business fields.

- [ ] **Step 3: Preserve malformed dates without inventing values**

Add a row whose raw date is `13/40/2025`. Assert the normalized `<date>_raw` column retains `13/40/2025`, the typed `<date>` column is `NULL`, and a `date_parse_failure_count` metadata value is returned for the table. The pipeline converts that count into a warning `QualityResult`; it does not discard the source row.

- [ ] **Step 4: Add all-four-table parameterized tests**

Test that master, device, patient, and narrative inputs produce normalized business-key columns, source provenance columns, and `dataset_snapshot_id`. Test both FDA date formats observed locally.

- [ ] **Step 5: Run normalization tests**

Run: `uv run pytest tests/unit/test_normalize.py -v && make lint && make typecheck`  
Expected: all table and date cases pass.

- [ ] **Step 6: Commit canonical normalization**

```bash
git add src/maude/ingestion/normalize.py tests/unit/test_normalize.py
git commit -m "feat: normalize bronze tables into canonical parquet"
```

---

### Task 7: Implement Blocking Quality Gates and Human-Readable Reports

**Files:**
- Create: `src/maude/quality/__init__.py`
- Create: `src/maude/quality/checks.py`
- Create: `src/maude/quality/report.py`
- Create: `tests/unit/test_quality.py`

**Interfaces:**
- Consumes: `TableResult` sequence and optional prior validated `SnapshotResult`.
- Produces: `run_quality_checks(tables, previous=None) -> tuple[QualityResult, ...]`, `has_blocking_failure(results)`, `render_quality_markdown(snapshot)`.

- [ ] **Step 1: Write failing quality-gate tests**

```python
from maude.domain.enums import QualityLevel
from maude.quality.checks import has_blocking_failure, row_conservation, row_count_drift


def test_row_conservation_blocks_unaccounted_rows() -> None:
    result = row_conservation(rows_seen=100, accepted=98, rejected=1)
    assert result.level is QualityLevel.BLOCKING
    assert result.passed is False


def test_row_count_drop_of_more_than_ten_percent_blocks() -> None:
    result = row_count_drift(current=89, previous=100)
    assert result.passed is False
    assert has_blocking_failure((result,)) is True


def test_row_count_drop_at_ten_percent_is_warning_not_failure() -> None:
    result = row_count_drift(current=90, previous=100)
    assert result.passed is True
```

- [ ] **Step 2: Implement explicit checks**

Implement:

- `row_conservation`: blocking equality check;
- `reject_fraction`: blocking when rejects exceed 0.5% of seen rows, warning above 0%;
- `row_count_drift`: blocking when current accepted rows are below 90% of the same prior source/table;
- `business_key_uniqueness`: blocking on duplicate canonical business keys;
- `required_table_set`: blocking unless master, device, patient, and narrative all exist;
- `orphan_fraction`: warning up to 1% and blocking above 1% for child `mdr_report_key` values absent from master; and
- `truncation_comparison`: blocking when a converted text file has below 90% of the archive member’s newline count.

Each result must include numerator, denominator, threshold, and observed value in `metrics`.

- [ ] **Step 3: Render deterministic reports**

`render_quality_markdown` must include snapshot/run identifiers, status, per-table source checksum and row counts, a blocking/warning table, and promotion outcome. Sort checks by level then name so snapshot diffs are stable.

- [ ] **Step 4: Reproduce the known patient conversion failure in a test**

Create a fixture with 100 archive rows and a converted file containing 2 rows. Assert `truncation_comparison` fails with observed ratio `0.02`. This is the regression test for the local `patient.txt` versus truncated `patient_UTF8.txt` condition.

- [ ] **Step 5: Run quality tests**

Run: `uv run pytest tests/unit/test_quality.py -v`  
Expected: conservation, drift, orphan, duplication, rejection, and truncation cases pass.

- [ ] **Step 6: Commit quality gates**

```bash
git add src/maude/quality tests/unit/test_quality.py
git commit -m "feat: gate MAUDE snapshots on data quality"
```

---

### Task 8: Persist Manifests and Promote Snapshots Atomically

**Files:**
- Create: `src/maude/ingestion/manifests.py`
- Create: `src/maude/ingestion/pipeline.py`
- Create: `tests/integration/test_pipeline.py`

**Interfaces:**
- Consumes: source paths, `Settings`, parser/normalizer/quality services.
- Produces: `ingest_snapshot(snapshot_id, sources, settings) -> SnapshotResult` and `load_manifest(path) -> SnapshotResult`.

- [ ] **Step 1: Write failing end-to-end promotion test**

Build four one-member ZIP fixtures for master, device, patient, and narrative. Include two related report keys and one valid row per table. Then assert:

```python
result = ingest_snapshot("fixture-2025-06", sources, settings)
assert result.status is RunStatus.PROMOTED
assert Path(result.promoted_path).exists()
assert not layout.staging.exists()
assert load_manifest(layout.manifest).model_dump() == result.model_dump()
```

- [ ] **Step 2: Write failing rollback test**

First create a valid promoted snapshot and a `current.json` pointer. Run a second snapshot with a narrative source missing `FOI_TEXT`. Assert the second run is `FAILED`, its staging/reject evidence remains available, and `current.json` still names the first snapshot.

- [ ] **Step 3: Implement atomic manifest writing**

Serialize Pydantic JSON to a sibling `.tmp` file, `flush`, `os.fsync`, and call `Path.replace` to publish. `load_manifest` validates through `SnapshotResult.model_validate_json`.

- [ ] **Step 4: Implement pipeline orchestration**

`ingest_snapshot` must:

1. reject a snapshot ID outside `[a-zA-Z0-9._-]+`;
2. create a `RUNNING` manifest before parsing;
3. process sources in deterministic `TableKind` order;
4. parse and normalize each table;
5. run all quality checks;
6. persist `FAILED` plus evidence when an exception or blocking result occurs;
7. atomically move validated silver output to `data/silver/<snapshot_id>`;
8. atomically update `data/manifests/current.json` only after promotion; and
9. return `PROMOTED` with timezone-aware completion time.

Re-running the same snapshot ID and identical source checksums returns the existing promoted manifest without rewriting Parquet. Reusing a snapshot ID with different checksums raises `SnapshotConflict`.

- [ ] **Step 5: Run integration tests**

Run: `uv run pytest tests/integration/test_pipeline.py -v`  
Expected: valid promotion, failed rollback, identical rerun, and snapshot conflict tests pass.

- [ ] **Step 6: Commit manifests and pipeline**

```bash
git add src/maude/ingestion/manifests.py src/maude/ingestion/pipeline.py tests/integration
git commit -m "feat: promote validated MAUDE snapshots atomically"
```

---

### Task 9: Build DuckDB Canonical Views and Report Documents

**Files:**
- Create: `src/maude/storage/duckdb.py`
- Create: `tests/integration/test_duckdb_views.py`

**Interfaces:**
- Consumes: promoted silver Parquet directory.
- Produces: `open_snapshot(snapshot_path) -> duckdb.DuckDBPyConnection` and `fetch_report_document(connection, report_id) -> ReportDocument`.

- [ ] **Step 1: Write failing joined-document test**

```python
def test_report_document_preserves_narrative_evidence_ids(promoted_snapshot: Path) -> None:
    connection = open_snapshot(promoted_snapshot)
    document = fetch_report_document(connection, "1")

    assert document.report_id == "1"
    assert document.reported_event_type == "D"
    assert document.product_codes == ("ABC",)
    assert document.narratives[0].text == "Pump stopped unexpectedly"
    assert document.narratives[0].evidence_id == "narrative:10"
    assert document.dataset_snapshot_id == "fixture-2025-06"
```

- [ ] **Step 2: Define immutable report-document models**

Add to `domain/models.py`:

```python
class NarrativeEvidence(BaseModel):
    model_config = ConfigDict(frozen=True)
    evidence_id: str
    text_type_code: str | None
    text: str
    source_filename: str
    source_line_number: int


class ReportDocument(BaseModel):
    model_config = ConfigDict(frozen=True)
    report_id: str
    reported_event_type: str | None
    date_received: date | None
    product_codes: tuple[str, ...]
    brand_names: tuple[str, ...]
    manufacturers: tuple[str, ...]
    narratives: tuple[NarrativeEvidence, ...]
    dataset_snapshot_id: str
```

- [ ] **Step 3: Register stable DuckDB views**

Open an in-memory DuckDB connection and create `reports`, `devices`, `patients`, and `narratives` views with `read_parquet`. Create `report_documents` with pre-aggregated device metadata but keep narratives one row per evidence ID; assemble nested Pydantic objects in Python to avoid losing provenance through string aggregation.

- [ ] **Step 4: Test missing and multi-child behavior**

Assert an unknown report raises `ReportNotFound`. Assert multiple devices and narratives are deduplicated, sorted by stable sequence/key, and never multiply narratives through an accidental device × narrative join.

- [ ] **Step 5: Run integration and full tests**

Run: `uv run pytest tests/integration/test_duckdb_views.py -v && make verify`  
Expected: all repository checks pass.

- [ ] **Step 6: Commit serving views**

```bash
git add src/maude/domain/models.py src/maude/storage/duckdb.py tests/integration/test_duckdb_views.py
git commit -m "feat: expose provenance-safe report documents"
```

---

### Task 10: Add the Local Audit CLI and Runbook

**Files:**
- Create: `src/maude/service/__init__.py`
- Create: `src/maude/service/audit.py`
- Modify: `src/maude/cli.py`
- Create: `tests/integration/test_cli_audit.py`
- Create: `tests/fixtures/README.md`
- Create: `docs/runbooks/local-ingestion.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: `Settings`, `ingest_snapshot`, quality report renderer.
- Produces: `maude audit-local`, `maude ingest-local`, and documented operator workflow.

- [ ] **Step 1: Write failing CLI tests**

```python
def test_audit_local_reports_truncated_patient_conversion(
    runner: CliRunner,
    local_fixture_root: Path,
) -> None:
    result = runner.invoke(app, ["audit-local", "--data-root", str(local_fixture_root)])
    assert result.exit_code == 1
    assert "patient_UTF8.txt" in result.stdout
    assert "truncation" in result.stdout.lower()


def test_ingest_local_prints_promoted_snapshot(
    runner: CliRunner,
    complete_archive_root: Path,
) -> None:
    result = runner.invoke(
        app,
        ["ingest-local", "fixture-2025-06", "--source-root", str(complete_archive_root)],
    )
    assert result.exit_code == 0
    assert "PROMOTED fixture-2025-06" in result.stdout
```

- [ ] **Step 2: Implement local source discovery without guessing**

`audit.py` must map exactly one archive to each required current table using the schema detector, report missing/ambiguous families, compare any sibling `*_UTF8.txt` conversion with its archive member, and return `LocalAuditResult` using the exact shared contract above. It must not select `add` files as substitutes for base files.

- [ ] **Step 3: Implement CLI commands**

`audit-local` prints archive name, checksum prefix, member, encoding, header/table, and conversion comparison. Exit 1 for a blocking finding.

`ingest-local SNAPSHOT_ID --source-root PATH` runs the audit, refuses blocking inputs, calls `ingest_snapshot`, writes `quality-report.md` beside the manifest, and prints the promoted path and counts. Add `--allow-archive-only` so an invalid hand conversion does not block when the original archive itself passes; log that the conversion was ignored.

- [ ] **Step 4: Document fixture and operator policies**

`tests/fixtures/README.md` states that fixtures must be synthetic or minimally transformed, contain no private enrichment, and include source-shape notes.

`docs/runbooks/local-ingestion.md` documents:

- `.env` setup pointing to `/Users/rushilrawat/MAUDE` as an example only;
- dry audit command;
- archive-only ingestion command;
- output locations;
- interpreting blocking versus warning results;
- retry behavior; and
- how to confirm `current.json` was not changed after failure.

Update the root README with a “First vertical slice” section linking to the runbook.

- [ ] **Step 5: Run all verification**

Run: `make verify`  
Expected: Ruff, formatting, mypy, and all tests pass.

- [ ] **Step 6: Run the read-only audit against the real local directory**

Run:

```bash
uv run maude audit-local --data-root /Users/rushilrawat/MAUDE
```

Expected: archives are identified, `patient_UTF8.txt` is reported as truncated relative to `patient.zip`, and no source file is modified.

- [ ] **Step 7: Run the first full local ingestion from original archives**

Run:

```bash
uv run maude ingest-local local-2025-06 \
  --source-root /Users/rushilrawat/MAUDE \
  --allow-archive-only
```

Expected: the four base archives are streamed, a validated snapshot is promoted, a quality report is written, and `current.json` references `local-2025-06`. If the local archive set fails completeness or referential-integrity gates, preserve the failed manifest and do not weaken thresholds; record the exact missing table/source requirement for Plan 2’s official redownload.

- [ ] **Step 8: Inspect outputs without checking them into Git**

Run:

```bash
git status --short
uv run maude audit-local --data-root /Users/rushilrawat/MAUDE
```

Expected: generated data does not appear in Git status; the audit remains read-only and repeatable.

- [ ] **Step 9: Commit CLI and documentation**

```bash
git add src/maude/cli.py src/maude/service tests docs/runbooks README.md
git commit -m "feat: audit and ingest local MAUDE archives"
```

---

## Final Verification and Handoff

- [ ] Run `make verify` and retain the passing output in the task handoff.
- [ ] Run `git status --short` and confirm only intentional tracked changes remain.
- [ ] Open the generated quality report and reconcile its table counts to the final `SnapshotResult` manifest.
- [ ] Query at least three real report documents through DuckDB and verify their narrative evidence IDs against source rows.
- [ ] Record elapsed time and peak resident memory for each source archive; these measurements set Plan 2 worker sizing rather than serving as arbitrary pass/fail gates.
- [ ] Start Plan 2 only after the local snapshot is either promoted or has a documented source-completeness failure that requires official FDA redownload.

## Deferred by Design

This plan does not install scikit-learn, LangChain, LangGraph, pgvector, FastAPI, Next.js, Prefect, or an LLM SDK. Adding those dependencies before the canonical data contracts pass would increase surface area without validating the project’s highest-risk foundation. Their exact integration belongs to the downstream plans named in Plan Boundary.
