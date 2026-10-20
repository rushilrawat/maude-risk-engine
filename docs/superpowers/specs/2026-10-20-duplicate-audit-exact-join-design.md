# MAUDE Duplicate Audit v2: Exact Thresholded Jaccard Join

**Date:** 2026-10-20

**Status:** Draft for review

## Context

The completed audit of snapshot `fda-current-43bedbf20c68c84b` scanned
4,243,225 narratives. It found 2,619 exact-duplicate groups crossing temporal
splits and no repeated report keys across splits. Its approximate near-duplicate
candidate search emitted 8,831,163 pairs, but 3,403,958 query narratives hit a
250-candidate cap. Because candidate IDs were selected in corpus order, the
reported near-pair set is incomplete and order-biased. It must not be used as a
complete leakage check or as a basis for excluding training records.

## Goal

Replace the current near-duplicate candidate generator with a disk-backed,
exact similarity-join algorithm that reports every eligible cross-split pair
whose Jaccard similarity over the existing unique word-trigram hash sets is at
least `0.8`. Exactness is with respect to that hashed feature representation.
The result must be reproducible, source-linked, and explicit about completion.

## Design

- Use a proven exact set-similarity join (PPJoin-style) over each narrative's
  unique word-trigram set: a deterministic global token order (document
  frequency ascending, hash ascending for ties), prefix filtering, length
  bounds, positional pruning where applicable, and exact Jaccard verification.
  The implementation must correctly handle temporal pair direction even when
  the later-split narrative has fewer shingles than its prior-split candidate.
  Candidate generation must not silently discard matches due to a fixed cap.
- Preserve temporal direction: validation narratives compare with training;
  test narratives compare with training and validation. Do not compare records
  from the same report or identical normalized-text hash as near duplicates.
  Narratives without a temporal split remain counted but are not compared.
- Keep the join and intermediate postings disk-backed in SQLite; stream output
  in deterministic report/evidence order. Do not retain all candidate pairs in
  Python memory.
- Exact joins can still have quadratic worst-case candidate growth. Check
  available disk space before starting and during execution; stop before free
  space falls below 10 GiB, publish no successful manifest, and report the last
  completed phase and observed free-space minimum.
- Publish results under a new audit version so the immutable v1 output remains
  intact. Record algorithm version, Jaccard threshold, snapshot/corpus hashes,
  counts, and artifact checksums in the v2 manifest.
- If the process cannot complete, do not publish a successful manifest. A
  published manifest means all required join and output phases completed.

## Verification

- Compare the implementation against a brute-force pairwise oracle on small
  fixtures, including boundary similarities below, at, and above `0.8`,
  varied set sizes, and both temporal-vs-length orderings.
- Prove all eligible temporal pair directions are included, no same-split or
  same-report pairs are emitted, and corpus order does not affect output.
- Run the full unit and integration suites plus Ruff and mypy.
- Rerun v2 on the pinned snapshot, verify artifact checksums by invoking the
  immutable audit path again, and report counts, runtime, peak scratch size,
  and remaining disk space. Do not start clustering until the result has been
  reviewed and the split-exclusion policy is recorded.

## Scope and limitations

This changes near-duplicate candidate completeness at the configured
Jaccard threshold; it does not establish that narratives describe the same
clinical event. Exact narrative matches and near-similarity pairs remain
research signals requiring review. This work does not alter FDA source data,
the temporal split boundaries, narrative cleaning, or the separate historical
data bootstrap requirement.

## Acceptance criteria

1. The fixture oracle and all repository checks pass.
2. The v2 manifest identifies the exact-join strategy, records zero candidate
   overflows, and marks the search exhaustive.
3. A full-snapshot v2 audit completes and its checksummed artifacts can be
   reopened and verified without recomputing the join.
4. The v1 audit remains unchanged; v2 is the only result considered for the
   subsequent leakage review.

## Reference

Xiao et al., “Efficient Similarity Joins for Near-Duplicate Detection,” WWW
2008, describes prefix filtering, length filtering, and positional filtering
for exact Jaccard-threshold joins: <https://archives.iw3c2.org/www2008/papers/pdf/p131-xiaoA.pdf>.
The implementation must independently verify the required temporal join
behavior against the brute-force fixture oracle; the paper's ordinary
self-join ordering cannot be assumed to cover this code's split-directed join.
