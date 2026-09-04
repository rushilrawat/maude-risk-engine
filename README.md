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
