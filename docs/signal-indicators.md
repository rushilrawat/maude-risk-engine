# Reported event and report-volume indicators

`maude build-signals <research-corpus-id>` creates deterministic indicators
from the disjoint promoted snapshots recorded in a combined research-corpus
manifest. Build that corpus from historical and current data together; a
current-year-only snapshot would fill the prior-year baseline with false zeros.
The output is a research and triage aid. It does not estimate incidence,
establish causality, or make a medical safety conclusion.

## Cohorts and event labels

- The cohort is the normalized FDA device report product code.
- A report is counted once per product code per calendar month. A report with
  multiple product codes contributes once to each code; a report without a code
  remains in a null-code cohort.
- FDA `EVENT_TYPE` is mapped directly: `DEATH` → `death`,
  `INJURY` → `injury`, `MALFUNCTION` → `malfunction`,
  and `OTHER` → `other`.
- Blank event types are `missing`; unmapped values are
  `unrecognized`. The raw source value is retained in
  `report_indicators.parquet`.

These are FDA-reported event categories. They are not clinical severity labels.

## Report-volume baseline

The latest complete month is the earlier of the latest month represented in the
source data and the month before the latest source-snapshot completion. The
builder requires at least 12 prior and 3 recent calendar months, with reports
present in every month of the combined window. For each product code, it
compares the latest three complete months with the preceding twelve:

```text
recent_average = recent_three_month_total / 3
baseline_median = median(monthly counts in preceding 12 months)
baseline_MAD = median(abs(monthly_count - baseline_median))
robust_z = (recent_average - baseline_median)
           / max(1.4826 * baseline_MAD, 1 report/month)
additional_reports = recent_three_month_total - 3 * baseline_median
report_volume_change = robust_z >= 3 and additional_reports >= 5
```

A product code with no report in a month is assigned zero for that month. The
builder rejects a whole-data month gap within the 15-month window. The
one-report scale floor handles a zero MAD without emitting an infinite score.
The manifest records source corpus and snapshot IDs, checksums, window dates,
formula constants, and output checksums.

## Outputs

The immutable output is stored under
`MAUDE_DATA_ROOT/features/<research-corpus-id>/reported-severity-volume-v1/`:

- `report_indicators.parquet`: source report ID, source snapshot ID,
  received date, product code, raw FDA event type, and mapped reported-event
  category.
- `monthly_cohorts.parquet`: report counts and event-type composition by
  product code and month.
- `volume_change.parquet`: recent and baseline counts, median/MAD, robust
  z-score, minimum-count check, and volume-change flag.
- `manifest.json`: research-corpus and source snapshot checksums, formulas,
  coverage dates, counts, and artifact checksums.

Build the combined corpus first from fully promoted historical and current
snapshots, then use the ID it prints:

```bash
uv run maude build-research-corpus fda-history-HISTORICAL_FINGERPRINT fda-current-CURRENT_FINGERPRINT
uv run maude build-signals <research-corpus-id>
```

Clustering-based novelty, cluster confidence, evidence density, and routing remain
separate work and require the historical corpus and evaluation gates.
