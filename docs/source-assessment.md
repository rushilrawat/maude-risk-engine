# FDA MAUDE source assessment

**Reviewed:** 2026-11-07

**Scope:** official FDA sources for medical-device adverse-event research.

## Decision

Keep the FDA MDR Data Files as the canonical ingestion source. They provide
downloadable MAUDE table archives, preserve the raw records needed for local
reconciliation, and are updated monthly. Use the official openFDA device/event
API as a freshness and coverage cross-check; do not append its records to the
bulk archive without matching report identities and measuring overlap. Treat
AEMS as the FDA's migration destination and add an adapter only after its
device-record export, schema, versioning, and identifiers have been verified
against MAUDE.

This preserves an FDA-only source policy while giving the project a path to
detect newer records between monthly archive releases.

## Evidence reviewed

- The [FDA MDR Data Files page](https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files)
  lists downloadable MAUDE archives updated monthly. It separates the master
  archive through 2025 from current-year base, add, and change archives, and
  still carries a future-tense notice about moving MAUDE data into AEMS during
  2026.
- The [openFDA device/event API](https://open.fda.gov/apis/device/event/)
  describes its records as sourced from MAUDE, with FDA-added annotations and
  JSON conversion. On the review date, its page reported weekly updates and
  coverage through 2026-10-07, with API coverage beginning in 2009. This is a
  useful freshness check, but it is not the same raw archive or full historical
  coverage.
- The [FDA AEMS overview](https://www.fda.gov/safety/fda-adverse-event-monitoring-system-aems)
  describes AEMS as an implemented cross-product system with a public dashboard.
  The [FDA launch announcement](https://www.fda.gov/news-events/press-announcements/fda-launches-new-adverse-event-look-tool)
  stated that AEMS would include real-time reports across FDA-regulated product
  categories by the end of May 2026. The reviewed material does not establish a
  versioned, bulk medical-device export that this repository has ingested and
  reconciled against MAUDE. FDA's current AEMS access path and device export
  contract therefore need an implementation-level check before adapter work.

## Implementation implications

1. Preserve the monthly MAUDE archive and immutable snapshot as the analytical
   source of record for now.
2. Record each source's retrieval time and source-reported data-through date.
   Alert when the newest available archive is older than the expected monthly
   cadence.
3. Add an FDA openFDA freshness check with bounded date windows. Store response
   metadata and compare report identifiers and date coverage; do not combine
   API and archive rows until overlap and update semantics are understood.
4. Reassess AEMS when FDA publishes a usable medical-device bulk/API contract.
   Before switching, run both adapters over a fixed period and report record
   counts, identifier overlap, field coverage, missingness, revisions, and
   retrieval reproducibility.
5. Keep counts labeled as submitted reports. MAUDE is passive surveillance;
   report counts alone do not establish event incidence, causation, or
   comparative device risk.

## Current project state

The repository has the monthly MAUDE bulk-file adapter and immutable snapshots.
The openFDA comparison, automated freshness alert, and AEMS adapter remain
future implementation work. The live data snapshot currently available to this
workspace ends on 2026-08-31, so it needs a fresh FDA refresh after the active
historical ingestion and duplicate-audit jobs release the SSD workload.
