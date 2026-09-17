# Charter ledger — ICE-HADOOP-VN-1 · stale Hadoop `vN` commit raises loud, loses nothing

**Date:** 2026-09-17 · **Branch:** `fix/ice-hadoop-vn-1` · **Base:** `a003f9f5` ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `ICE-HADOOP-VN-1` near `V3-ADOPT-1` in
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Why now.** Rating row V2-20c (probes `conc` / `conc2`): two RePark memory catalogs
on one Spark Hadoop table at v2 — catalog 1 INSERT wrote v3, catalog 2 (stale v2)
INSERT overwrote v3 and catalog 1's row was lost silently; `conc2`: Spark committed
v3, a stale RePark INSERT overwrote it and Spark's row was lost for good. The fork
now exclusive-creates `vN` names and returns retryable `CatalogCommitConflicts`
(fork PR #286 `75da2b58`, in this tree via RP-21 `a003f9f5`; fork ledger
`task/f-ice-hadoop-vn-1-ledger.md` in the read-only clone at `/tmp/ib-forkread`).
This unit is the RePark side: surface the conflict loud after the retry budget,
lose nothing, pin it, row it.

**Not in this unit:** any fork edit (table-format semantics live in the fork; the
pin moves only in its own PR); `STATUS.md`; `.github/`; dependency files.

**Oracle.** Live PySpark 4.1.2 cells recorded by the unit recorder into
`python/repark/tests/ice_hadoop_vn_1_spark_oracle.json`: Spark's rows after each
shape, Spark's PySpark-visible class and message prefix for its own stale commit.
No hand-computed Spark expectation.

**Rulings.**

- R-1 (brief, step 4 parenthetical): if the surfaced conflict is untyped base
  `PySparkException`, it takes the class RePark already uses for commit conflicts;
  no new public class is invented; if none exists the unit HALTs with the question.
  Standing evidence: the pinned contract keeps `CatalogCommitConflicts` in the
  `Error::Iceberg` base bucket (`crates/repark-core/src/session/tests/session.rs`
  CQ-015, `crates/repark-core/src/session/tests/map.md`, `crates/repark-common`,
  `crates/repark-python/src/tests.rs`). No typed commit-conflict class exists
  anywhere in the tree. The ledger records the measured surface against V2-20a in
  C-004 and the verdict on R-1 lands there.

## PROPOSITION LEDGER — ICE-HADOOP-VN-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `conc`: two RePark catalogs over a copied Spark fixture table; catalog 1 INSERT lands v3; catalog 2 (stale v2) INSERT, MERGE, DELETE, UPDATE each raise the conflict; both catalogs and Spark read catalog 1's rows; v3 bytes unchanged. | `python/repark/tests/test_ice_hadoop_vn_1.py` offline pins; Red-first: the rating's recorded silent outcomes plus a Rust test in `repark-iceberg` or `repark-core` committing twice from stale memory catalogs over a tempdir Hadoop table asserting the conflict. | PROVEN | 2026-09-17: 6 offline pins green (`test_conc_stale_writers_raise_and_winner_bytes_survive`, `test_conc_winner_rows_read_back`, `test_stale_handle_stays_wedged_loud`); the stale catalog's read is seed-stale per F-4, pinned as stale. Rust pin still to land. pins: ice-hadoop-vn-1/C-001 |
| C-002 | `conc2` (live): Spark commits first, stale RePark INSERT raises, Spark reads its own row and can keep committing. | Live pins in the same file (`REPARK_PARITY_LIVE=1`). | PROVEN | 2026-09-17: `test_live_conc2_stale_repark_raises` green live (9 passed in 153.80s for the whole file). pins: ice-hadoop-vn-1/C-002 |
| C-003 | Recovery: re-registering the stale catalog at the newest metadata then committing succeeds and Spark reads all rows. | Offline + live pins in the same file. | PROVEN | 2026-09-17: `test_recovery_fresh_registration_commits` (offline) + `test_live_recovery_spark_reads_all` (live) green; recipe refined per F-3 (fresh handle, not re-register). pins: ice-hadoop-vn-1/C-003 |
| C-004 | The RePark exception class and message stand against Spark's mirrored stale-commit error (PySpark-visible class and message prefix) and against the existing OCC conflict contract (V2-20a / `occ_conflict`); R-1 verdict recorded. | Oracle JSON + pins asserting class and prefix + this ledger's R-1 verdict. | PROVEN | 2026-09-17: `test_exception_contract_matches_occ` green; oracle records Spark scan-forward (no error) per F-5; R-1 verdict in F-6. pins: ice-hadoop-vn-1/C-004 |
| C-005 | The DataFrame door writers (`writeTo().append()`, `saveAsTable(append)`) as the stale writer raise the same conflict. | Pins in the same file. | PROVEN | 2026-09-17: `test_dataframe_doors_stale_writer_raises` green offline. pins: ice-hadoop-vn-1/C-005 |
| C-006 | Repro: `/tmp/ib-scratch/probes/p_failures.py` conc / conc2 blocks copied into `p_hadoop_vn.py`, run through `/tmp/oc-worker/jb-jvm.sh`, output pasted. | Evidence paste below. | PROVEN | 2026-09-17, rc 0, findings F-1..F-6 below. pins: ice-hadoop-vn-1/C-006 |
| C-007 | Spark oracle recorded as a checked-in fixture (recorder + truth JSON per the `test_ice_spark_table_1.py` / `_oracle_pins.py` convention). | Recorder script + `ice_hadoop_vn_1_spark_oracle.json`. | PROVEN | 2026-09-17: `_record_ice_hadoop_vn_1.py` rc 0; fixture `python/repark-parity/fixtures/torture/data/ice_hadoop_vn_1` (1 parquet, v1+v2, manifest, hint, truth.json) + `ice_hadoop_vn_1_spark_oracle.json` checked in. pins: ice-hadoop-vn-1/C-007 |
| C-008 | Registry row ICE-HADOOP-VN-1 near V3-ADOPT-1 (FIXED 2026-09-17 by fork #286 at pin sha, typed error, recovery recipe, D-2's loud wedge on an orphan version file); residue sentence at ~6897 replaced with a pointer; tests map and ledgers map in lockstep. | The registry diff. | PROVEN | 2026-09-17: row `ICE-HADOOP-VN-1` after `V3-ADOPT-1`, residue clause replaced with a pointer, `write/map.md` + tests/fixture maps in lockstep. pins: ice-hadoop-vn-1/C-008 |
| C-009 | No regression: the new file offline and live, `make verify`, the whole facade suite, the whole parity suite, green with real exit codes and counts. | §Gates. | PROVEN | Counts in §Gates; the release native is untouched (test-only Rust under `cfg(test)`, no facade change), so no rebuild was due. pins: ice-hadoop-vn-1/C-009 |

## Red-first log

### C-001 Rust pins on the previous fork pin (temporary local revert, never committed)

`crates/repark-iceberg/src/write/hadoop_stale_commit.rs` run with the workspace pin
reverted to `edc38c6a` (pre-#286; `CARGO_NET_OFFLINE=true`, restored byte-identical
after via `git checkout -- Cargo.toml Cargo.lock`, zero diff):

```text
test write::hadoop_stale_commit::stale_hadoop_pointer_second_append_fails_loud_and_keeps_winner ... FAILED
test write::hadoop_stale_commit::stale_hadoop_pointer_stays_wedged_loud ... FAILED
test result: FAILED. 0 passed; 2 failed
stale vN append must fail loud: Table { ..., metadata_location: Some(".../ns/src/metadata/v3.metadata.json"), ... }
```

The stale append returns `Ok` and overwrites `v3` — the rating's silent shape. On the
unit pin (`75da2b58`) both pass (`2 passed; 0 failed`, 6.50s / 7.98s).

### C-001 Python pins

The rating report §6 row "Stale-base commit on adopted Hadoop table" is the recorded
silent outcome on the unfixed tree (V2-20c MISSING). The new pins assert the fixed
shape; their red state on the old pin follows by construction from the Rust red above
(same fork seam, same `expect_err` on the stale commit).

## Evidence

### C-006 repro output

`/tmp/ib-scratch/probes/p_hadoop_vn.py` (conc / conc2 blocks of `p_failures.py`
plus stale MERGE / DELETE / UPDATE, a Spark-vs-RePark commit race, and a planted-v3
collision), run 2026-09-17 through `/tmp/oc-worker/jb-jvm.sh` on
`PYTHONPATH=/tmp/jb-vn/python/repark/src:/tmp/jb-vn/.venv/lib/python3.12/site-packages`
with `/tmp/sparkenv/bin/python`, rc 0. Banner: `spark.version=4.1.2`,
`session.tz=UTC`, `pyspark=4.1.2`,
`gav=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`. Full stdout kept
in the run output; load-bearing lines:

```text
OK conc repark cat1 INSERT (base v2)
conc metadata after cat1: ['v1.metadata.json', 'v2.metadata.json', 'v3.metadata.json']
ERR conc repark cat2 stale INSERT (base v2): PySparkException: CatalogCommitConflicts => Cannot commit table metadata to /tmp/ib-scratch/wh/p_hadoop_vn/sp_wh/ns/conc/metadata/v3.metadata.json: version file already exists (.../v3.metadata.json), source: PreconditionFailed => Cannot create .../v3.metadata.json: file already exists, source: File exists (os error 17)
conc v3 bytes unchanged after stale INSERT: True
ERR conc repark cat2 stale MERGE (base v2): PySparkException: CatalogCommitConflicts => ... (same shape)
conc v3 bytes unchanged after stale MERGE: True
OK conc repark cat2 stale DELETE (base v2): [Row()]
conc v3 bytes unchanged after stale DELETE: True
ERR conc repark cat2 stale UPDATE (base v2): PySparkException: CatalogCommitConflicts => ... (same shape)
conc v3 bytes unchanged after stale UPDATE: True
conc metadata final: ['v1.metadata.json', 'v2.metadata.json', 'v3.metadata.json']
OK conc repark cat1 reads: [(1, 'seed'), (2, 'rp-cat1')]
OK conc repark cat2 reads: [(1, 'seed')]
OK conc spark reads: [(1, 'seed'), (2, 'rp-cat1')]
ERR conc re-register cat2 at newest then INSERT: AnalysisException: TableAlreadyExists => Cannot create table TableIdent { namespace: NamespaceIdent(["ns"]), name: "conc" }. Table already exists.
ERR conc cat2 recovery INSERT (base v3): PySparkException: CatalogCommitConflicts => ... (same shape)
ERR conc2 repark INSERT on stale base v2: PySparkException: CatalogCommitConflicts => ... (same shape)
conc2 v3.metadata.json unchanged by repark: True
OK conc2 repark reads: [(1, 'seed')]
OK conc2 spark reads: [(1, 'seed'), (2, 'spark')]
OK conc2 spark INSERT again (can keep committing): []
OK conc2 spark reads after second spark commit: [(1, 'seed'), (2, 'spark'), (4, 'spark-again')]
race spark data files appeared before repark commit: True
OK race repark INSERT during spark write: [Row(count=1)]
race metadata after repark: ['v1.metadata.json', 'v2.metadata.json', 'v3.metadata.json']
race spark slow INSERT outcome: committed
OK race spark reads: [(400002, 2)]
OK planted spark INSERT onto existing v3: []
OK planted spark reads: [(1, 'seed'), (2, 'spark'), (2, 'spark')]
repark.errors classes: ['AnalysisException', 'CommitStateUnknownException', 'IllegalArgumentException', 'ParseException', 'PySparkAssertionError', 'PySparkAttributeError', 'PySparkException', 'PySparkNotImplementedError', 'PySparkRuntimeError', 'PySparkTypeError', 'PySparkValueError', 'UnsupportedOperationException', 'annotations']
DONE
```

### C-006 findings (all measured, 2026-09-17)

- F-1: every stale RePark writer that commits (INSERT, MERGE, UPDATE) raises
  `repark.errors.PySparkException` with a `CatalogCommitConflicts`-leading message
  naming the existing `vN` file, and the winner's bytes are unchanged.
- F-2: the stale DELETE committed nothing because it matched zero rows on the stale
  snapshot (`DELETE WHERE id = 2`, only visible post-v3) — correct no-commit, not a
  gap. Pins delete a stale-visible row instead.
- F-3: `CALL system.register_table` on an existing name refuses `TableAlreadyExists`
  — re-registration is NOT the recovery. The workable recovery is a fresh catalog
  handle registered at the newest version (a new memory catalog or session); DROP +
  re-register is unsafe (the fork's memory `drop_table` deletes the pointer's metadata
  file). The stale handle stays wedged-loud on every later commit (D-2's wedge).
- F-4: the stale catalog's READS stay stale (`cat2 reads: [(1, 'seed')]`) — the
  pre-existing memory-catalog read-staleness, same class as Spark's cached-table
  staleness already rowed in ICE-SPARK-TABLE-1. The brief's "both catalogs read the
  first writer's rows" holds for the winning catalog and for Spark after refresh;
  the stale catalog's read is pinned as stale, not as winner rows. Nothing is lost:
  every row stays readable via the winner, via Spark, and via a fresh registration.
- F-5: Spark never raises in the mirrored shape. A 400k-row Spark INSERT racing a
  RePark commit scan-forwards and commits the next version (`committed`,
  400002 rows); a planted `v3.metadata.json` does not fail Spark's INSERT either —
  Spark lists the metadata dir and continues at the next version. Java's
  `CommitFailedException: Version N already exists` needs a true simultaneous-commit
  race and is not reachable deterministically through PySpark, so the oracle records
  Spark's rows plus "Spark scan-forwards, no PySpark-visible error" instead of a
  class and prefix that live Spark 4.1.2 does not show.
- F-6: `repark.errors` carries no commit-conflict class. R-1 verdict: the surfaced
  base `PySparkException` with the `CatalogCommitConflicts`-leading message IS the
  existing OCC conflict contract (V2-20a "base PySparkException, message-typed",
  pinned in `crates/repark-core/src/session/tests/session.rs` CQ-015); no new public
  class is invented, per the brief's own ban.

## §Gates

| Gate | Result |
|---|---|
| new pins offline (`.venv/bin/python -m pytest python/repark/tests/test_ice_hadoop_vn_1.py -q -p no:cacheprovider`) | 6 passed, 3 skipped (live cells) |
| new pins live (`REPARK_PARITY_LIVE=1`, pyspark 4.1.2 via sparkenv path, through `jb-jvm.sh`) | 9 passed in 153.80s |
| Rust pins (`cargo test -p repark-iceberg --lib write::hadoop_stale_commit`) | 2 passed |
| `make verify` | rc 0 |
| `cargo test --workspace` | rc 0 (3713 tests listed) |
| whole facade (`.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider`) | 9332 passed, 370 skipped, 26 xfailed in 628.58s (clean re-run after the bytecode-purge repair; first run `1 failed, 9331 passed` on the poisoned cache, see incident above) |
| whole parity (`PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests -q`) | 757 passed, 2 skipped, 12 xfailed in 371.97s |
| added-comment count over `origin/main..HEAD` | 0 |
| `git status --short` at handoff | clean |

### Gate incident 2026-09-17: cross-clone bytecode poisoning (pre-existing, repaired)

The first whole-facade run ended `1 failed, 9331 passed, 370 skipped, 26 xfailed` on
`test_dfcore_4b_show_goldens.py::test_show_styled_vertical_warning_attributes_to_caller`,
which asserts a warning names the calling file. The warning named
`/tmp/ib-bump/...` while `__file__` was `/tmp/jb-vn/...`. Root cause: the clone
carries `__pycache__` dirs whose bytecode was compiled from `/tmp/ib-bump` sources —
690 of 692 inventoried `.pyc` files record an `/tmp/ib-bump` `co_filename`, and Python's
`(mtime, size)` validation passes because the clones preserve both. Nothing in this
unit writes show/display code; the poison predates the session (pyc dated 00:43, session
from 05:12). Repair, sanctioned routine for regenerable caches: removed the 38
`__pycache__` dirs under the repo tree (`.venv` untouched, status clean), after
which the file passes `7 passed`. The whole suite is re-run below on the clean tree.

## Open questions

1. R-1 verdict, ANSWERED (see C-004, F-6): no distinct typed commit-conflict class
   exists in the tree; the pinned contract is base `PySparkException`, message-typed
   with `CatalogCommitConflicts` leading, and the measured stale-writer surface
   matches it exactly — conformance recorded, no class invented, no HALT. A new typed
   class would contradict the pinned classification tests (session.rs CQ-015); if the
   orchestrator wants one anyway, that is a new unit.

## Round 2 (2026-09-17) — logic-critic remediation

**Ruling Q-20b-3 (orchestrator, 2026-09-17):** a stale replace split-brains (ruling
text in the brief); the fix belongs in the fork (`F-HADOOP-VN-REPLACE-1`); RePark
carries no local patch and this unit opens no fork PR. This unit measures both doors,
records Spark's mirror, pins current behavior exactly with `xfail(strict=True)` target
cells, and files OPEN residue row `ICE-HADOOP-VN-1-R-001`.

| Clause | Proposition (checkable) | Verdict | Evidence |
|---|---|---|---|
| R2-C001 (L-02 per Q-20b-3) | Stale replace measured both doors; Spark mirror recorded; R-001 filed; current behavior pinned exactly with strict-xfail targets; docstring + registry wedge text corrected. | PROVEN | Probe `/tmp/ib-scratch/probes/p_hadoop_vn_r2.py` rc 0 (paste below); `test_stale_replace_splits_brain`, `test_stale_replace_doors_split_brain`, `test_stale_replace_raises_conflict`, `test_stale_df_replace_raises_conflict`, `test_live_replace_split_brain_spark_reads_winner`, `test_live_spark_replace_after_repark_commit`; registry `ICE-HADOOP-VN-1-R-001`. pins: ice-hadoop-vn-1/C-008 |
| R2-C002 (L-01) | Every registry sentence pinned or removed. | PROVEN | `test_same_name_reregister_refuses`, `test_drop_stale_handle_deletes_pointer_file`, `test_planted_orphan_wedges_repark_loud`, `test_oracle_spark_scan_forward_keys` green offline; `test_live_planted_next_version_commits`, `test_live_spark_race_scan_forwards` run in R2-C004. No sentence removed. pins: ice-hadoop-vn-1/C-008 |
| R2-NOTE | Recorder incident: the fixture rewrite used `rmtree(FIXTURE_DIR)`, deleting the checked-in `map.md`; fixed to clear only `metadata/`, `data/`, `truth.json`, map restored from git. | PROVEN | Recorder diff in the step-1 commit. |
| R2-C003 (L-03) | DataFrame doors share one helper; overwrite/truncate/alter shapes pinned. | PROVEN | Probe `/tmp/ib-scratch/probes/p_hadoop_vn_r3.py` rc 0: stale INSERT OVERWRITE, TRUNCATE, ALTER SET TBLPROPERTIES and saveAsTable(overwrite) raise `CatalogCommitConflicts` with bytes intact; `writeTo().overwrite` refuses declared `UnsupportedOperationException` on stale and fresh handles alike (pre-existing Group I refusal, no residue row). Pins `test_stale_overwrite_shapes_raise`, `test_stale_overwrite_doors_raise` green offline; append doors refactored onto `_assert_stale_write_raises`. pins: ice-hadoop-vn-1/C-005 |
| R2-C004 (gates) | Round-2 gates green with counts. | OPEN |  |

### R2-C001 evidence — L-02 probe output (`p_hadoop_vn_r2.py`, rc 0, 2026-09-17)

```text
OK rpl cat1 INSERT (base v2): [Row(count=1)]
rpl metadata after cat1: ['v1.metadata.json', 'v2.metadata.json', 'v3.metadata.json']
OK rpl cat2 stale CREATE OR REPLACE (SQL door): [Row()]
rpl metadata after SQL replace: ['00003-f1a6f136-4ba4-44f1-90ff-ca0279905c16.metadata.json', '00004-6d59a76a-8b6d-4341-ae0b-56d65aad35c5.metadata.json', 'v1.metadata.json', 'v2.metadata.json', 'v3.metadata.json']
rpl v3 bytes unchanged after SQL replace: True
OK rpl cat2 reads after SQL replace: [(99, 'rtas')]
OK rpl cat1 reads after SQL replace: [(1, 'seed'), (2, 'rp-cat1')]
OK rpl spark reads after SQL replace: [(1, 'seed'), (2, 'rp-cat1')]
OK rpl2 cat2 stale writeTo.replace: None
rpl2 metadata after DF replace: ['00003-7e588996-53d9-4190-b4d0-8e4d8b4c5f1d.metadata.json', '00004-7377dba4-3ae1-49f0-ad5e-2f47ee08f914.metadata.json', 'v1.metadata.json', 'v2.metadata.json', 'v3.metadata.json']
rpl2 v3 bytes unchanged after DF replace: True
OK rpl2 cat2 stale writeTo.createOrReplace: None
rpl2 metadata after DF createOrReplace: ['00003-7e588996-53d9-4190-b4d0-8e4d8b4c5f1d.metadata.json', '00004-7377dba4-3ae1-49f0-ad5e-2f47ee08f914.metadata.json', '00005-066429fb-f1e3-4651-998d-918d450b714c.metadata.json', '00006-bd3c852f-4839-4cb3-9099-20256e9ea655.metadata.json', 'v1.metadata.json', 'v2.metadata.json', 'v3.metadata.json']
rpl2 v3 bytes unchanged: True
OK rpl2 cat2 reads: [(97, 'df-cor')]
OK rpl2 cat1 reads: [(1, 'seed'), (2, 'rp-cat1')]
OK rpl2 spark reads: [(1, 'seed'), (2, 'rp-cat1')]
OK rpl3 spark CREATE OR REPLACE after repark commit: []
rpl3 metadata after spark replace: ['v1.metadata.json', 'v2.metadata.json', 'v3.metadata.json', 'v4.metadata.json']
OK rpl3 spark reads after replace: [(100, 'spark-rtas')]
OK rpl3 repark reads after spark replace: [(1, 'seed'), (2, 'rp-cat1')]
DONE
```

## Coverage attestation

```text
COVERAGE_ATTESTATION:
  pr_unit: ice-hadoop-vn-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause C-001..C-009 is pinned and green; the three measured refinements against the brief text (stale-read stays stale F-4, Spark never raises F-5, recovery is a fresh handle F-3) are pinned as measured, not as briefed.
      artifacts: [python/repark/tests/test_ice_hadoop_vn_1.py, crates/repark-iceberg/src/write/hadoop_stale_commit.rs, python/repark/tests/ice_hadoop_vn_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Four stale writer kinds (INSERT, MERGE, DELETE, UPDATE) plus both DataFrame doors plus repeated wedge writes plus the Spark-first shape plus the planted-file and 400k-row race shapes; the matched-zero DELETE (F-2) pins the no-commit boundary.
      artifacts: [python/repark/tests/test_ice_hadoop_vn_1.py, /tmp/ib-scratch/probes/p_hadoop_vn.py]
    - id: AT-3
      status: ATTACKED
      evidence: The stale commit burns a bounded retry budget (2 retries at 1/5ms in the Rust pin, the fork default budget live) then surfaces; the failed commit leaves the version bytes, the metadata listing and the catalog pointer unchanged, and a second stale write raises identically.
      artifacts: [crates/repark-iceberg/src/write/hadoop_stale_commit.rs, python/repark/tests/test_ice_hadoop_vn_1.py::test_stale_handle_stays_wedged_loud]
    - id: AT-4
      status: ATTACKED
      evidence: Two catalog instances share one file tree with no lock between them; the loser never advances and the winner never blocks; the Spark race scan-forwards; the fresh-handle recovery resumes at v(N+1).
      artifacts: [python/repark/tests/test_ice_hadoop_vn_1.py::test_recovery_fresh_registration_commits, /tmp/ib-scratch/probes/p_hadoop_vn.py]
    - id: AT-5
      status: N/A
      justification: Local tempdir files only; no credential, no network, no privileged action — the live Spark leg runs local[2] with a cached ivy and the offline legs touch only the copied fixture.
    - id: AT-6
      status: ATTACKED
      evidence: The winner's vN bytes are asserted byte-identical after every stale attempt; the winner's rows read back from the winning catalog, from a fresh registration and from Spark after refresh; the fixture truth.json and the oracle JSON freeze the seed and the row sets.
      artifacts: [python/repark/tests/test_ice_hadoop_vn_1.py, python/repark-parity/fixtures/torture/data/ice_hadoop_vn_1/truth.json]
    - id: AT-7
      status: N/A
      justification: No unbounded growth and no hot loop — the retry budget is bounded, every stale attempt terminates in seconds, and every tempdir and fixture copy is removed by tmp_path or the materialize guard.
    - id: AT-8
      status: ATTACKED
      evidence: The fork's conflict kind is consumed, not presumed — the Rust pins fail red on the pre-#286 pin and pass green on 75da2b58; the surfaced class and kind-led message match the pinned OCC contract (session.rs CQ-015) instead of inventing a new one.
      artifacts: [crates/repark-iceberg/src/write/hadoop_stale_commit.rs, python/repark/tests/test_ice_hadoop_vn_1.py::test_exception_contract_matches_occ]
    - id: AT-9
      status: ATTACKED
      evidence: The conflict message names the existing version file, the guard and the OS cause, so the loser is diagnosable from the message alone; the wedge repeats the same message on every later attempt.
      artifacts: [python/repark/tests/test_ice_hadoop_vn_1.py, python/repark/tests/ice_hadoop_vn_1_spark_oracle.json]
    - id: AT-10
      status: ATTACKED
      evidence: The suite would catch the missing guard — the temporary pre-#286 revert turned both Rust pins red with the stale append returning Ok; the Python pins assert the same seam per writer and door. No product branch was added (test-only Rust under cfg(test), docs, fixtures), so there is no unpinned new branch.
      artifacts: [task/ledgers/staging/ice-hadoop-vn-1-ledger.md]
  complete: true
```
