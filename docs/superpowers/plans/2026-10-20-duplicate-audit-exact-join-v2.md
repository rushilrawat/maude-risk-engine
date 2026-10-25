# MAUDE Duplicate Audit v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` or `superpowers:subagent-driven-development` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the capped, order-biased near-duplicate search with an exhaustive disk-backed join that emits every eligible cross-temporal-split pair at Jaccard similarity `>= 0.8` over the existing word-trigram hash sets.

**Architecture:** Keep the public `build_duplicate_audit` entry point and exact-duplicate/report-key outputs. Build a deterministic token order using document frequency in the eligible training/validation candidate corpus, and SQLite-backed postings only for those prior splits. Order target-only shingles at frequency zero, then process packed target shingles and joins in bounded batches using exact prefix and length bounds, safe positional pruning, and exact Jaccard verification. Write accepted pairs to SQLite for deterministic streaming; publish a new v2 directory atomically only after all phases and checksums succeed.

**Tech Stack:** Python 3.12, SQLite, Pydantic, pytest, Ruff, mypy

**Spec path:** `docs/superpowers/specs/2026-10-20-duplicate-audit-exact-join-design.md`

## Global Constraints

- Keep `narrative-duplicate-audit-v1` immutable; v2 writes to a separate versioned output directory.
- Similarity means Jaccard over the existing unique 64-bit word-trigram hashes, not clinical-event equivalence.
- Validation compares only with training; test compares only with training and validation. Exclude same-report and identical-normalized-text pairs; count but do not compare undated narratives.
- Candidate generation must have no fixed candidate cap and must not retain all postings or candidate pairs in Python memory.
- Results are complete only when the join, all output artifacts, and checksums finish. Never publish a successful manifest after an exception or disk-space stop.
- Check free space before work and during long phases; stop before available space drops below 10 GiB, clean only this run's staging directory, and report the last completed phase and minimum observed free space.
- Preserve existing user changes in the working tree, including the uncommitted shingle-cache work. Do not stage unrelated hunks or raw/generated data.
- Do not start clustering or claim clinical risk/incidence/causality from duplicate similarity results.

## Review Focus

- Prove candidate recall against a brute-force oracle, especially when the later-split set is smaller than its earlier-split candidate.
- Check threshold boundary behavior, deterministic global ordering, temporal direction, exclusion rules, and corpus-order independence.
- Verify the v1 directory is untouched, v2 manifests are exhaustive with zero candidate overflow, and disk-space failure cannot leave a success manifest.
- Treat the full pinned-snapshot run as an acceptance gate, not as proof that similarity represents the same medical event.

---

### Task 1: Add backward-compatible algorithm metadata

**Files:**
- Modify: `src/maude/domain/models.py`
- Modify: `tests/unit/test_manifests.py`

**Interfaces:**
- `DuplicateAuditManifest.algorithm_version: str = "legacy-v1"` deserializes existing v1 manifests without rewriting their stored content; v2 sets an explicit algorithm identifier when the exact join is integrated.

- [x] **Step 1: Add failing compatibility tests**

  Add a valid `DuplicateAuditManifest` fixture using the current v1 fields, then assert validation supplies the legacy algorithm identifier. Add a round-trip assertion that an explicit algorithm identifier is preserved.

- [x] **Step 2: Run focused tests and verify RED**

  Run: `uv run pytest tests/unit/test_manifests.py -q`

  Expected: the compatibility test fails because the model does not yet accept or supply `algorithm_version`.

- [x] **Step 3: Implement only the versioned metadata change**

  Add the backward-compatible model field with the legacy default. Do not change the audit version, publish v2 output, or change candidate-generation behavior in this task.

- [x] **Step 4: Run focused regression tests**

  Run: `uv run pytest tests/unit/test_manifests.py -q`

  Expected: PASS; v1 manifest fields still deserialize and explicit v2 metadata round-trips.

### Task 2: Implement disk-backed exact candidate generation

**Files:**
- Modify: `src/maude/features/duplicates.py`
- Test: `tests/unit/test_duplicate_join.py`
- Test: `tests/integration/test_duplicate_audit.py`

**Interfaces:**
- Internal index-building helpers persist shingle document frequencies, deterministic token ranks, and per-narrative token positions in SQLite.
- An internal pair iterator consumes the SQLite index and yields eligible `(prior_id, later_id, exact_jaccard)` matches without accumulating candidate pairs in Python memory.
- Keep the current public v1 builder on its existing path until Task 3 switches publication to v2; Task 2 tests the new join helper directly.

- [x] **Step 1: Add oracle-based failing tests**

  In the new unit test, define a small brute-force oracle independent of the production candidate generator. Cover Jaccard values just below, exactly at, and above `0.8`; varied set sizes; a later-split narrative smaller than its matching training narrative and the reverse; validation-to-training and test-to-training/validation directions; same-report and equal-normalized-text exclusions; and no same-split or undated near pairs. Assert the production result set exactly equals the oracle result set. Include shuffled insertion orders and assert identical semantic output.

- [x] **Step 2: Run and verify RED**

  Run: `uv run pytest tests/unit/test_duplicate_join.py -q`

  Expected: the exact join helper is absent or misses oracle pairs, including the temporal-versus-length ordering case.

- [x] **Step 3: Implement the SQLite-backed join**

  Expand only training/validation candidate narratives into disk-backed postings; derive a deterministic order by prior-corpus document frequency then hash; assign deterministic candidate positions; and create split-directed lookup indexes. Read future target shingles from packed narrative rows, treating target-only tokens as frequency zero, and process targets in bounded batches. Apply mathematically safe Jaccard length bounds, prefix filtering, and positional upper bounds before exact set-intersection verification. Keep `near_pairs` on disk and preserve the required temporal orientation in emitted IDs. Leave the public v1 builder unchanged in this task; Task 3 integrates the tested helper into v2 and removes the old bottom-16-signature and 250-candidate path.

- [x] **Step 4: Verify oracle, ordering, and scaling properties**

  Run: `uv run pytest tests/unit/test_duplicate_join.py tests/integration/test_duplicate_audit.py -q`

  Expected: PASS; the generator returns every oracle pair, never emits excluded pairs, and does not retain global posting/candidate dictionaries in Python.

### Task 3: Add guarded v2 publication and deterministic audit artifacts

**Files:**
- Modify: `src/maude/features/duplicates.py`
- Modify: `src/maude/domain/models.py`
- Modify: `src/maude/cli.py`
- Modify: `tests/unit/test_duplicate_candidate_batching.py`
- Test: `tests/unit/test_duplicate_join.py`
- Test: `tests/integration/test_duplicate_audit.py`

**Interfaces:**
- A specific insufficient-disk exception reports the current phase and minimum observed free bytes without creating a success manifest.
- `build_duplicate_audit` publishes exact, report-key, and near-pair artifacts plus checksums and the exhaustive v2 manifest only after successful completion.
- The completed v2 manifest records the exact thresholded Jaccard strategy, `near_candidate_overflow_documents == 0`, and `near_candidate_search_is_exhaustive is True`.

- [x] **Step 1: Add failing disk/publication tests**

  Extend the small audit fixture to assert the v2 output path, algorithm version, exhaustive status, and zero overflow. Create a sentinel in a fixture v1 output directory, run v2, and assert the sentinel is unchanged. Inject disk-usage readings to test a preflight stop and a mid-run stop below 10 GiB. Assert the exception reports the last completed phase/minimum free space, the staging directory is cleaned, and no success manifest or final v2 directory is published. Add assertions that artifact order is deterministic across corpus orderings and that a second invocation reopens the same manifest and verifies checksums without rebuilding.

- [x] **Step 2: Run and verify RED**

  Run: `uv run pytest tests/integration/test_duplicate_audit.py -q`

  Expected: tests fail because the disk guard and exact-v2 publication contract do not exist.

- [x] **Step 3: Implement guarded, atomic publication**

  Introduce the v2 audit/algorithm identifiers only now that the exact generator exists. Check free space before creating work and before bounded index, join, and output writes using a conservative growth estimate while preserving the 10 GiB reserve. Track completed phase and minimum observed space. On failure, let the current temporary staging directory clean itself and propagate a concise diagnostic through the CLI. On success, stream JSONL artifacts in stable report/evidence order, hash every artifact, write the complete v2 manifest, and atomically promote staging. Verify existing outputs by manifest inputs and artifact hashes.

- [x] **Step 4: Run the focused integration suite**

  Run: `uv run pytest tests/unit/test_duplicate_join.py tests/integration/test_duplicate_audit.py -q`

  Expected: PASS, including failure cleanup, immutable output, checksum validation, deterministic order, and v1 preservation.

### Task 4: Validate repository and the pinned FDA snapshot

**Files:**
- Modify if needed: `docs/ISSUE_BACKLOG.md`
- Modify if needed: `docs/PROJECT_GUIDE.md`

- [x] **Step 1: Run all repository checks**

  Run: `make verify`

  Expected: pytest, Ruff lint/format checks, and strict mypy all pass.

- [x] **Step 2: Preflight and run the pinned full-snapshot audit**

  The pinned run was safely stopped before crossing the reserve. At the final check, free space was 18,848,731,136 bytes; the ranked-postings-index phase conservatively anticipated 10,420,647,360 bytes of writes while preserving 10,737,418,240 bytes (10 GiB). The last completed phase was `corpus-index`; staging was cleaned. This is a completed guarded attempt, not full-snapshot acceptance.
  Run: `uv run maude audit-duplicates fda-current-43bedbf20c68c84b`

  Proceed only if the configured data root contains the pinned corpus and free space is above the required reserve. Before each bounded disk-writing batch, check that available space can absorb its conservative growth estimate while preserving the 10 GiB reserve. Record runtime, minimum free space, peak scratch size, final output size, narrative/pair counts, and checksums. If guarded for disk space, report the last phase and do not weaken the reserve or publish partial results to the final v2 directory.

- [x] **Step 3: Verify immutable reopen and v1 preservation**

  The guard-stop path left no staging directory or v2 final output. Existing v1 files still match their manifest checksums. Full-snapshot v2 acceptance remains outstanding.
  If the full run succeeds, run the same command again; verify it reopens the existing v2 output, all artifact checksums match, the manifest is exhaustive with zero overflow, and the v1 output hashes are unchanged. If the disk guard stops the run, verify there is no success manifest and leave full-snapshot acceptance outstanding.

- [ ] **Step 4: Update only evidence-backed project status and commit**

  Backlog and project guide now describe the exhaustive fixture-verified algorithm and the pinned-run limitation without claiming full-corpus results. Commit is pending review and staging only v2-related changes.
  Update backlog/guide status only for work verified above. Stage only v2-related changes, preserving any pre-existing user edits. Commit with message: `feat: add exact disk-backed duplicate audit`.

## Final acceptance

The implementation is complete when the fixture oracle and `make verify` pass and v1 artifacts remain unchanged. The roadmap milestone is complete only when the pinned-snapshot run completes and reopens with verified checksums and an exhaustive manifest with zero candidate overflow. If the 10 GiB guard stops it, report that the implementation passed local checks but full-snapshot acceptance remains outstanding. A clustering step remains separate and requires review of the v2 leakage results and a recorded split-exclusion policy.
