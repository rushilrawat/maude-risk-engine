# maude-risk-engine
Reproducible FDA MAUDE reporting-signal analysis and triage system for identifying medical device failure patterns. It is not a clinical risk estimator.

## Setup

Install the project and development dependencies with:

```bash
uv sync --all-extras
cp .env.example .env
```

Run the full verification suite with:

```bash
make verify
```

## First vertical slice

The first vertical slice audits canonical local FDA MAUDE archives, streams
them into versioned Parquet, applies blocking quality gates, and exposes
provenance-safe report documents. Follow the [local ingestion runbook](docs/runbooks/local-ingestion.md)
for the read-only audit, archive-only override, output locations, and recovery
workflow.
