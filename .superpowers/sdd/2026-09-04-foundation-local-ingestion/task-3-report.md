# Task 3 Report: Identify Tables and Enforce Source Schemas

## Scope

Added canonical `TableSpec` definitions and bounded filename/header detection in
`maude.ingestion.schemas`, including stable source-column normalization and strict
`spec_for` uniqueness enforcement. Added focused unit coverage for successful
detection, missing columns, filename-family requirements, and ambiguity.

## TDD evidence

### RED

Command:

```text
uv run pytest tests/unit/test_schemas.py -v
```

Result before implementation:

```text
collected 0 items / 1 error
ModuleNotFoundError: No module named 'maude.ingestion'
```

### GREEN

Command:

```text
uv run pytest tests/unit/test_schemas.py -v
```

Result:

```text
collected 9 items
9 passed in 0.14s
```

## Required checks

- `uv run ruff check src/maude/ingestion tests/unit/test_schemas.py` — All checks passed.
- `uv run mypy src/maude` — Success: no issues found in 10 source files.
- `uv run pytest` — 19 passed in 0.19s.

## Notes

Detection first narrows by filename family, rejects unknown or multiply matching
families, then requires every schema column after normalization. It reports all
missing required columns. `spec_for` raises `SchemaMismatch` unless exactly one
registered spec matches the requested kind.
