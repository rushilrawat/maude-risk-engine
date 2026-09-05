# Task 9 implementation report

## Files

- `src/maude/domain/models.py`: immutable `NarrativeEvidence` and `ReportDocument` models.
- `src/maude/domain/__init__.py`: public model exports.
- `src/maude/storage/duckdb.py`: in-memory DuckDB snapshot views and document fetcher.
- `src/maude/storage/__init__.py`: public storage API exports.
- `tests/integration/test_duckdb_views.py`: joined-document, missing-child, dedup/order,
  missing-artifact, and special-path coverage.

## RED / GREEN

- RED: before implementation, `uv run pytest tests/integration/test_duckdb_views.py -v`
  failed during collection with `ModuleNotFoundError: No module named 'maude.storage.duckdb'`.
- GREEN: the focused integration suite passes (5 tests).

## Checks

- `uv run pytest tests/integration/test_duckdb_views.py -v`: passed.
- `uv run pytest -q`: passed (106 tests).
- Changed-file Ruff check: passed.
- Changed-file Ruff format check: passed.
- `uv run mypy --cache-dir /private/tmp/maude-mypy-cache src/maude`: passed.
- `make verify`: existing `tests/unit/test_parser.py` formatting drift causes the format
  stage to fail; Ruff check itself passes and the unrelated file was not modified.

## Commit

`feat: expose provenance-safe report documents`

## Concerns

- Device metadata aliases currently prioritize the canonical FDA device manufacturer and
  brand columns when multiple aliases are present.
- Promoted snapshots are expected to use the canonical Silver schema and table artifacts;
  missing files or columns fail with table/path-specific messages before a connection is
  returned.
