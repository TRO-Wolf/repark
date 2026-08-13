# V-4 — write-path partition-key VALUE audit

**Date:** 2026-08-13 · **Lane:** V-4 · **Branch:** `grok/v4-partition-values` ·
**Worktree:** `/tmp/grok-v4` · **Freeze SHA:** `8d325d4f47f46154bd954dc515d717434517fca5`
**Charter:** `BRIEF-v4-partition-values.md` + conductor-7 Addendum A2/A4 +
`TZ4-DESIGN.md` §4 (Q5=A: this unit stays small; value audit after TZ-4 PR-2 / #85).

**This ledger is the audit record.** It is not a fix wave. The registry file is not
edited (V-5 owns it; §6 is paste-true).

---

## 0. Scope

**In.** New corpus `python/repark/tests/test_partition_value_audit.py` + record
driver `_record_partition_value_goldens.py`. CTAS + INSERT across the transform ×
type matrix. Each content row pins (a) partition VALUES Spark 4.1.2 writes, (b)
values repark writes, (c) Iceberg spec field-name/transform + `files`/`partitions`
record_count summaries. Read-only against the engine.

**Out.** Fixes >5 lines; fork edits; `_live_parity.py` / registry; tz representation
(#85 closed). V-1's files CLOSED (`normalize.rs` is the PARTITIONED-BY extractor and
still CLOSED). A4 grant-by-evidence only in `create_table.rs` / `ctas.rs` /
`crates/repark-sql/src/partitioning.rs` / `partitioned_ctas.rs`, ≤5 lines,
pin-proven.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Differential corpus | `python/repark/tests/test_partition_value_audit.py` | 30 rows + lifecycle + classifiers + budget + CP-1 |
| Record driver | `python/repark/tests/_record_partition_value_goldens.py` | Re-derive Spark halves (Iceberg GAV) |
| Tests map / package map / task map | lockstep | |
| This ledger | `task/v4-partition-values-ledger.md` | |

**Grant-by-evidence:** **no.** No eligible unowned REPARK file needed a ≤5-line
pin-proven correction. Named findings are fork-side or TZ-8 (chartered disclose).

### 1.1 Swept transform × type matrix (budget 24–34 → **30**)

| # | Name | Family | Form | Zone | Outcome |
|---|---|---|---|---|---|
| 1 | `carry_identity_int_ctas` | carry_check | CTAS | UTC | **PASS** data+meta |
| 2 | `carry_identity_int_insert` | carry_check | INSERT | UTC | **PASS** data+meta |
| 3 | `carry_identity_string_ctas` | carry_check | CTAS | UTC | **PASS** data+meta |
| 4 | `carry_identity_date_ctas` | carry_check | CTAS | UTC | **PASS** data+meta |
| 5 | `carry_identity_timestamp_ctas` | carry_check | CTAS | UTC | **DIVERGE** meta F-V4-1; data values match, type F-V4-2 |
| 6 | `carry_identity_timestamp_insert` | carry_check | INSERT | UTC | **DIVERGE** meta F-V4-1; data values match, type F-V4-2 |
| 7 | `carry_bucket_int_ctas` | carry_check | CTAS | UTC | **PASS** (murmur3 slot 0 for {1,2,15}) |
| 8 | `carry_truncate_int_ctas` | carry_check | CTAS | UTC | **PASS** (0 / 10) |
| 9 | `carry_truncate_string_ctas` | carry_check | CTAS | UTC | **PASS** (a / b / aa) |
| 10 | `carry_years_ts_ctas` | carry_check | CTAS | UTC | **PASS** values 0/54 (UTC-epoch); data type F-V4-2 |
| 11 | `carry_year_singular_ts_ctas` | carry_check | CTAS | UTC | **PASS** same 0/54 (`year` aliases Iceberg `years`, not SQL `year`) |
| 12 | `carry_months_ts_ctas` | carry_check | CTAS | UTC | **PASS** values 0/648/653; data type F-V4-2 |
| 13 | `carry_days_ts_ctas` | carry_check | CTAS | UTC | **PASS** UTC dates (NY-boundary → 2024-01-01, not 2023-12-31) |
| 14 | `carry_hours_ts_ctas` | carry_check | CTAS | UTC | **PASS** hours-from-1970 (0/24/29/473356/477348) |
| 15 | `carry_years_ts_insert` | carry_check | INSERT | UTC | **PASS** same 0/54 as CTAS |
| 16 | `carry_years_date_ctas` | carry_check | CTAS | UTC | **PASS** 54/55 |
| 17 | `load_year_ts_identity_new_york_ctas` | load_bearing | CTAS | NY | **PASS** y=2023 for NY-boundary instant |
| 18 | `load_year_ts_identity_tokyo_ctas` | load_bearing | CTAS | Tokyo | **PASS** y=2023 for Tokyo-boundary instant |
| 19 | `load_year_ts_identity_utc_ctas` | load_bearing | CTAS | UTC | **PASS** control |
| 20 | `load_year_ts_identity_new_york_insert` | load_bearing | INSERT | NY | **PASS** same 2023 slot as CTAS |
| 21 | `load_date_format_ts_new_york_ctas` | load_bearing | CTAS | NY | **PASS** `2023-12-31` |
| 22 | `load_zoneless_year_ts_identity_new_york_ctas` | load_bearing | CTAS | NY | **PASS** post-#85 naive → y=2024 |
| 23 | `tz8_cast_ts_as_date_identity_new_york_ctas` | tz8 | CTAS | NY | **DIVERGE** Spark 2023-12-31 / repark 2024-01-01 |
| 24 | `tz8_to_date_ts_identity_new_york_ctas` | tz8 | CTAS | NY | **DIVERGE** same class as CAST |
| 25 | `refuse_bucket_zero` | refuse | CTAS | UTC | **REFUSE** both (Spark `Unsupported width`; repark `> 0`) |
| 26 | `refuse_truncate_zero` | refuse | CTAS | UTC | **REFUSE** both |
| 27 | `refuse_bucket_negative` | refuse | CTAS | UTC | **REFUSE** both |
| 28 | `refuse_unknown_transform` | refuse | CTAS | UTC | **REFUSE** both |
| 29 | `refuse_hours_on_date` | refuse | CTAS | UTC | **REFUSE** both (`Invalid source type`) |
| 30 | `refuse_void_transform` | refuse | CTAS | UTC | **REFUSE** both (`Transform is not supported`) |

**Counts:** pass **20** · diverge **4** (2× F-V4-1 identity-timestamptz meta + 2× TZ-8) ·
refuse **6**. F-V4-2 (timestamptz Arrow annotation `+00:00` vs `UTC`) rides the
timestamp-carrying data halves; it is a type rider, not a partition-VALUE miss.

### 1.2 `partitioned_ctas.rs:446-488` verify

Still present, still asserts Iceberg `years|months|days|hours` spec names + distinct
partition counts over 1970-01-01T00 / 1970-01-02T00 / 1970-01-02T05 (1 / 1 / 2 / 3).
This corpus **adds the VALUES** (0/54 years, 0/648/653 months, UTC dates, hour
offsets) and the year-boundary instant that would have been 2023 under a session-zone
mis-implementation. Not a fix hunt — carry-check confirmed.

### 1.3 Lifecycle helper

`run_write_lifecycle` / `extract_meta` live in the test module (recipe SSOT the
record driver imports). Source fixtures: ints / strings / dates / instants /
temporal / load / naive. Meta is the uniform `(surface, spec, slot, record_count)`
table over `.files` + `.partitions`.

### 1.4 Re-derive command

```bash
uv sync --locked --extra record \
    --extra numpy --extra pandas --extra polars --extra ml-ext \
    --no-install-package repark
# Hold /tmp/grok-jvm-record.lock MARKER=v4-record
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_partition_value_goldens.py
# first-record dump: add --dump (never edits the corpus)
```

Record basis: `master("local[2]")`, ANSI on, shuffle=2, UI off, GAV
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` (CP-8 derived).

---

## 2. Decisions

1. **Identity of SQL `year(ts)` is a projected column, not `PARTITIONED BY (year(ts))`.**
   The Spark door maps `year`/`years` both to Iceberg `Transform::Year` (UTC-epoch).
   Row 11 pins that alias. The load-bearing hole is `PARTITIONED BY (y) AS SELECT
   year(ts) AS y`.
2. **Metadata tables are the common spec/value surface.** Memory catalog does not
   leave a readable `metadata.json` tree. `.files.partition` + `.partitions.partition`
   + `record_count` pin (a)(b)(c).
3. **Timestamptz identity meta is a named finding, not a silent skip.** Spark
   projects the slot; repark's files/partitions inspect refuses
   `partition field type Timestamptz is not supported`. Fork CLOSED.
4. **TZ-8 stays a disclosure.** CAST/to_date partition keys write UTC dates on
   repark and session-zone dates on Spark. No fix (A2).
5. **void() is a both-refuse, not a split.** Live Spark 4.1.2 + Iceberg 1.11 also
   rejects `PARTITIONED BY (void(id))`.
6. **Grant-by-evidence = no.** See §5.

---

## 3. Named findings

### F-V4-1 — timestamptz identity partition metadata projection

- **Input.** `CREATE TABLE … PARTITIONED BY (ts) AS SELECT id, ts` (instant-typed
  TIMESTAMP / Iceberg `timestamptz`).
- **Spark.** `.files.partition` / `.partitions.partition` yield
  `{"ts":"2024-01-01T04:30:00.000000Z"}` etc.
- **repark.** Write succeeds; data round-trips. Metadata read raises
  `FeatureUnsupported => partition field type Timestamptz is not supported in the
  data_file metadata projection`.
- **Home.** Fork inspect (`data_file` metadata projection). **CLOSED** to this lane
  even for one line.
- **Pin.** `test_partition_value_row[carry_identity_timestamp_ctas]` (and INSERT twin).

### F-V4-2 — timestamptz Arrow annotation after Iceberg read

- **Input.** Any table whose data column is Iceberg `timestamptz`.
- **Spark `toArrow`.** `timestamp[us, tz=UTC]`.
- **repark `to_arrow`.** `timestamp[us, tz=+00:00]` (fork read mapping).
- **Values** match. Not a partition-VALUE miss. Type rider on every timestamp-carrying
  data half.
- **Home.** Fork Arrow mapping. CLOSED.

### F-V4-3 — TZ-8 CAST(ts AS DATE) / to_date as identity partition key

- **Input.** NY session, instant `2024-01-01T04:30Z`.
- **Spark.** Partition date `2023-12-31` (session zone).
- **repark.** Partition date `2024-01-01` (UTC calendar).
- **Charter.** Disclose / refuse pin, **no fix** (A2 = TZ-8).
- **Pin.** `tz8_cast_ts_as_date_identity_new_york_ctas` /
  `tz8_to_date_ts_identity_new_york_ctas`.

**Load-bearing hole status:** after #85, SQL `year(ts)` / `date_format` identity
partitions under NY/Tokyo **match Spark**. The H-1a carry-check PASSES. Not a finding.

---

## 4. Gate evidence

| Gate | Exit | Notes |
|---|---|---|
| `pytest python/repark/tests/test_partition_value_audit.py` | **0** | 39 passed (30 rows + 9 harness) |
| `make verify` | **0** | 2026-08-13 in-worktree |
| `make preflight` | **0** | 2026-08-13 in-worktree (verify + facade + audit + workflow lint) |

Oracle dump: `/tmp/v4-spark-dump.txt` (30 rows, 0 dump mismatches). Lock event in §7.

---

## 5. Grant-by-evidence

**No.** Completeness critic: the ≤5-line A4 window was considered against
`create_table.rs`, `ctas.rs`, `crates/repark-sql/src/partitioning.rs`,
`partitioned_ctas.rs`. F-V4-1/F-V4-2 live in the fork (CLOSED). TZ-8 is chartered
disclose. Load-bearing cells are green. No pin in this corpus reds for want of a
≤5-line edit in an eligible file. V-1 files were not opened.

---

## 6. Registry-shaped handoff (paste-true; V-5 owns the file)

Do **not** edit `docs/spark-sql-iceberg-parity.md` from this unit.

- **repark** — identity-partition of an Iceberg `timestamptz` column writes and
  round-trips rows, but `table.files` / `table.partitions` refuse to project the
  partition struct (`FeatureUnsupported`: Timestamptz not supported in the
  data_file metadata projection).
- **Apache Spark** — the same CTAS/INSERT projects
  `{"ts":"2024-01-01T04:30:00.000000Z"}` (and the companion instant) from
  `.files` / `.partitions`. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[carry_identity_timestamp_ctas]`
  and
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[carry_identity_timestamp_insert]`
- **Rationale** — BACKLOG, fork inspect. F-V4-1. Not a TZ-4 representation miss
  (data values match). Do not "fix" in repark by skipping the meta read.

- **repark** — Iceberg `timestamptz` columns export `timestamp[us, tz=+00:00]`.
- **Apache Spark** — `timestamp[us, tz=UTC]`. Instants match. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[carry_years_ts_ctas]`
  (type rider; every timestamp-carrying data half).
- **Rationale** — BACKLOG / DECLARED alias. F-V4-2. Fork read mapping
  (`Timestamp(µs, "+00:00")`). Not a partition-VALUE divergence.

- **repark** — `CAST(ts AS DATE)` / `to_date(ts)` as an identity partition key
  writes the **UTC** calendar date (`2024-01-01` for `2024-01-01T04:30Z` under
  `America/New_York`).
- **Apache Spark** — writes the **session-zone** date (`2023-12-31`). *(oracle:
  recorded.)*
- **Pin** —
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[tz8_cast_ts_as_date_identity_new_york_ctas]`
  and
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[tz8_to_date_ts_identity_new_york_ctas]`
- **Rationale** — BACKLOG, TZ-8. UTC annotation does not close date-cast (TZ4-DESIGN
  §2.4). A2: disclose, no fix this unit.

---

## 7. Lock events

| When | Event |
|---|---|
| 2026-08-13T16:36:11-04:00 | Observed `/tmp/grok-v1-first-released` (V-1 first release). |
| pre-record | Lock absent. No local `pyspark`/`SparkSubmit` driver. V-2/V-3 still coding (no lock). |
| 2026-08-13T16:46:25-04:00 | Acquired `/tmp/grok-jvm-record.lock` `MARKER=v4-record` pid=108575. |
| 2026-08-13T16:46–16:47 | `--dump` of 30 Spark+Iceberg rows → `/tmp/v4-spark-dump.txt`. Exit 0. |
| immediately after dump | Marker-verified `MARKER=v4-record`; **rm** lock. RELEASE-ON-EXIT. |
| stale-rm | **none**. |

---

## 8. Provocation transcripts (CP-1)

Classifier arms are committed tests (not one-shot). Both arms of split and of
content-disclosure:

```
test_split_classifier_converged_arm PASSED
test_split_classifier_regression_arm PASSED
test_content_disclosure_classifier_converged_arm PASSED
test_content_disclosure_classifier_regression_arm PASSED
```

Monkeypatch target: `run_write_lifecycle` (the real lifecycle the row test calls).

---

## 9. Deviations FLAGGED

- **Ruff RUF002:** the corpus docstring says "transform x type" (ASCII x), not the
  multiplication sign, so the lint stays green. Ledger/charter still use the
  conventional "×".
- **void():** charter allowed a Spark-accepts / repark-refuses split. Live Spark
  4.1.2 + Iceberg 1.11 also refuses. Honest both-refuse pin.
- **Identity timestamp VALUES** are pinned on the data path (the partition key
  *is* the column). Meta projection is F-V4-1, not a silent skip.

---

## 10. Actor build summary

- Slice: pin-only partition-value audit corpus + record driver + ledger/maps.
- Risk tier: **standard** (test-only; write-path values; no engine edit).
- Nearest AGENTS.md: root `AGENTS.md`; `python/repark/tests/map.md`; `task/map.md`.
- Files touched: the two new test files, three maps, this ledger.
- Tests: 39 collected / 39 passed (local facade pytest of this module).
- Verify: `make verify` exit 0; `make preflight` exit 0.
- Known limits: F-V4-1/2/3 named; grant-by-evidence no; fork CLOSED.
- Clause trace: A2 both halves (carry-check + load-bearing + TZ-8); A4 considered
  and not fired; JVM FIFO after V-1; one PR; no merge; no origin fetch.
- Dependency trees: fork read-authority only (no edit).
- Enumeration: 30-row matrix = carry + load + tz8 + refuse; pin count = 30.

---

## 11. ACC + Critic-4 (claims_critic)

**Mode:** sequential hat-switch in one session (no `isolation: worktree` spawn).
Independence is weaker than fresh explore agents; artifacts were re-read from the
tree, not from Actor memory.

### Critic-1 (Quality / Bugs)

Context break executed; attacking artifacts, not memory.

- Verdict: **CLEAN** after one remediation cycle.
- Q-001 (S2, test adequacy): F-V4-1 Spark meta golden was optional in the budget
  pin (`if repark_meta_error_needle is None`). **REMEDIATED** — every content row
  now requires `spark_meta`.
- Q-002 (S2, oracle drift): record driver used `Table.equals` (order-sensitive)
  while the suite uses `assert_frames_equal`. **REMEDIATED** — driver now uses
  the parity comparator.
- Test-coverage skeptic: each family has a name-gated pin; years UTC-epoch and
  NY year() 2023 are semantics-gated. Classifier arms proven by monkeypatch.
- Enumeration: 30 pins = 30 matrix cells.
- Crates: n/a (no `crates/` edit).
- Corpus taxonomy: CP-1 (classifiers reachable), CP-2 (name/semantics gates),
  CP-4 (refuse needles are the refusing component's), CP-5 (§6 paste-true),
  CP-8 (GAV derived), CP-10 (budget 24–34 / 30), CP-11 (claim scoped to facade
  `sql()` CTAS+INSERT). Null-report on CP-3/6/7/9/12.
- Oracle spot-check (≥3 rows): **deferred this pass** — V-2 holds
  `/tmp/grok-jvm-record.lock` (`MARKER=v2-record`) after our dump release. First
  dump of all 30 rows is on disk (`/tmp/v4-spark-dump.txt`). Will re-hold after
  V-2 if still needed before SQM.

### Critic-2 (Security / Safety)

Context break executed; attacking artifacts, not memory.

- Verdict: **CLEAN**.
- Attacked: tempfile warehouses + `finally` drop; no AWS; no secrets in dump
  (values are synthetic instants); GAV fetched at record-time only; CI stays
  JVM-free. Atomicity n/a (no publish/commit engine path in this diff).
- Null report: no injection surface (SQL is fixture-built, not user input);
  no lockfile / credential edits.

### Critic-4 (Claims / Record)

Context break executed; attacking artifacts, not memory.

- Verdict: **CLEAN** pending CL-IDENTITY on the commit (`%ae` after commit).
- CL-COUNT: 30-row table + 20/4/6 = 30. Collect-only = 39 tests.
- CL-MANDATE: A2 both halves present; A4 named no; V-1 files untouched;
  registry untouched; `_live_parity.py` untouched.
- CL-GHOST: `partitioned_ctas.rs` temporal pin still at the cited test;
  §6 node ids match `pytest --collect-only`.
- CL-VACUOUS: refuse rows supply a real CREATE and pin the refusing needle.
- CL-QUANT: "UTC-epoch EXPECTED" is scoped to Iceberg years/months/days/hours,
  not to SQL year().
- Grant-by-evidence ≤5-line: **no eligible diff**. Completeness critic: the
  only reds are fork (CLOSED) or TZ-8 (chartered disclose).
