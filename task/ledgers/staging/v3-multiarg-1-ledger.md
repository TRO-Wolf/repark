# Unit ledger — V3-MULTIARG-1 · multi-argument transforms DECLARED out of 1.x

## Round 1 (2026-09-18)

**Date:** 2026-09-18 · **Branch:** `docs/v3-multiarg-1` · **Base:** `origin/main` ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `V3-MULTIARG-1` (rating row V3-05).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Owner ruling 2026-09-18 (binds this unit), quoted:**

> Multi-argument transforms are NOT built in 1.x. Declare them, with a card for later.

**Why docs plus one pin.** There is no product change: every engine surface already
refuses the shape loudly, and Spark 4.1.2 DDL cannot create it either, so there is no
live table to read. The unit measures the three refusals on the release native and in
one short-lived JVM, pins them, files the dated DECLARED registry row, and cards the
post-1.x work whose FIRST STEP is a Java-API fixture.

## PROPOSITION LEDGER — V3-MULTIARG-1 — 2026-09-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The SQL-door refusal is measured and pinned: `PARTITIONED BY (bucket(4, id, name))` refuses with `AnalysisException` carrying `expects (numBuckets, column), got 3 argument(s)`. | `test_sql_door_refuses_multiarg_bucket`; §1 verbatim commands. | **PROVEN** | Release native: `repark.errors.AnalysisException: Error during planning: CTAS PARTITIONED BY \`bucket(…)\` expects (numBuckets, column), got 3 argument(s): [4, id, name]` (§1a). The DataFrame door has no multi-argument spelling: `functions.bucket(numBuckets, col)` takes 2 positional arguments, a 3-argument call fails in Python with `TypeError: bucket() takes 2 positional arguments but 3 were given` (§1a). The native ANSI door does not reach the transform: `USING iceberg` is a parse error there (`ParseException: Expected: end of statement, found: USING`), so the pin sits on the Spark/Iceberg SQL door (§1a). Pin green: 3 passed (§4). |
| C-002 | Spark 4.1.2 cannot create the table: the same DDL refuses, and the refusal is recorded in a committed fixture plus a recorder. | `test_spark_oracle_refused_the_multiarg_ddl` over `v3_multiarg_1_spark_oracle.json`; `_record_v3_multiarg_1.py` with `--warehouse`. | **PROVEN** | One JVM under `jvm-lock.sh` (`/tmp/sparkenv/bin/python`, Hadoop catalog): `pyspark.errors.exceptions.captured.IllegalArgumentException: Cannot convert transform with more than one column reference: bucket(4, id, name)` (§1b, first and only line). Fixture committed beside the pins; the recorder re-derives the cell and exits non-zero on drift (§4). |
| C-003 | A foreign table carrying `"source-ids": [1, 2]` refuses loud, never silently: at register or at read, with class and message pinned. | `test_register_table_with_source_ids_refuses_at_register`; §1 verbatim commands. | **PROVEN** | Release native: `register_table` refuses with `repark.errors.PySparkException: DataInvalid => Failed to parse json string, source: data did not match any variant of untagged enum TableMetadataEnum` — the fork models only the singular `source-id`, so the whole document fails the metadata parse (§1c). The follow-up `SELECT` finds no table (`AnalysisException: table 'mc7.ns.m' not found`), which is the consequence of the failed register, not a second path. The unmodified single-`source-id` control registers and reads 0 rows cleanly, which proves the refusal is the `source-ids` shape (§1c). Pin green (§4). |
| C-004 | A dated DECLARED registry row names the ruling, all four measured answers, the pins, and the card. | Registry row `V3-MULTIARG-1` in `docs/spark-sql-iceberg-parity.md`. | **PROVEN** | Row `V3-MULTIARG-1` sits beside the V3 transform rows (after `V3-VARIANT-SHRED-1`): RePark SQL-door refusal, DataFrame-door absence, foreign-metadata register refusal, Spark DDL refusal, pins, card link, owner ruling 2026-09-18. |
| C-005 | A post-1.x card carries the goal, the NOT-in-1.x ruling, and FIRST STEP = the Java-API fixture plus the fork asks. | Card `task/roadmap/mid-term/v3-multiarg-1.md` plus the directory `map.md` entry. | **PROVEN** | Card filed 2026-09-18: goal (read, then write, multi-argument transforms), NOT in 1.x (owner ruling 2026-09-18), FIRST STEP (Java-API `UpdatePartitionSpec` / `PartitionSpec.builderFor` fixture, measured first), then the three fork asks (spec parsing, transform evaluation, pruning), draft clauses C-001…C-004. `map.md` entry added. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Measured on the release native in `.venv` (verbatim)

1a. SQL door (Spark/Iceberg dialect — the door that owns Iceberg DDL):

```text
$ .venv/bin/python -c "… s.sql('CREATE TABLE mc.ns.t (id INT, name STRING) USING iceberg PARTITIONED BY (bucket(4, id, name))') …"
CLASS: repark.errors.AnalysisException
MSG: Error during planning: CTAS PARTITIONED BY `bucket(…)` expects (numBuckets, column), got 3 argument(s): [4, id, name]
```

1a. DataFrame door (`functions.bucket` signature is `(numBuckets, col)`):

```text
CLASS: builtins.TypeError
MSG: bucket() takes 2 positional arguments but 3 were given
```

1a. Native ANSI door (`repark.sql`, context only — `USING` is not its spelling):

```text
CLASS: repark.errors.ParseException
MSG: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 46")
```

1b. Spark 4.1.2 + Iceberg 1.11.0, one JVM via `/tmp/oc-worker/_lib/jvm-lock.sh`
(Hadoop catalog, warehouse under a fresh temp dir):

```text
SPARK DDL CLASS: pyspark.errors.exceptions.captured.IllegalArgumentException
SPARK DDL FIRST LINE: Cannot convert transform with more than one column reference: bucket(4, id, name)
```

1c. Foreign v3 metadata (RePark-written v3 document with the one partition field
rewritten to `"source-ids": [1, 2]`, `"transform": "bucket[4]"`, zero snapshots):

```text
REGISTER CLASS: repark.errors.PySparkException
REGISTER MSG: DataInvalid => Failed to parse json string, source: data did not match any variant of untagged enum TableMetadataEnum
READ CLASS: repark.errors.AnalysisException
READ MSG: Error during planning: table 'mc7.ns.m' not found
CONTROL REGISTER: ok
CONTROL READ: ok, rows= 0
```

The control is the byte-identical document with the singular `"source-id": 1`: it
registers and reads cleanly, so the refusal is the `source-ids` shape. Nothing
answered silently; no defect to stop on.

## 2. Red proof (the pins refuse, they do not answer)

The three pins assert refusals, so each is red when the behaviour it names is absent:
`test_sql_door_refuses_multiarg_bucket` fails if the CREATE commits (no raise);
`test_register_table_with_source_ids_refuses_at_register` fails if the register
lands (no raise); `test_spark_oracle_refused_the_multiarg_ddl` fails if the fixture
cell carries any other class or message. The fixture cell is the verbatim live-Spark
refusal from §1b, not a hand-written expectation.

## 3. Out of scope (observed, not worked)

- The native ANSI door has no `USING iceberg` spelling at all (§1a context); giving
  it one is a separate unit, not this declaration.
- The Java-API fixture (card FIRST STEP): unmeasured — no second JVM was spent on it
  in this round (one-JVM rule).
- Fork `source-ids` parsing, multi-source transform evaluation, pruning: card clauses.

## 4. Gates

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_v3_multiarg_1.py -q -p no:cacheprovider` | 0 — 3 passed |
| `.venv/bin/ruff check` on the two new Python files | 0 — all checks passed |
| `.venv/bin/ruff format --check` on the two new Python files | 0 — both formatted |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/lb-build origin/main` | 0 hits (§5) |

Per the brief's machine rule, `make verify`, `make preflight`, `make ci`, the facade
suite and the parity harness were deliberately not run (docs-plus-pin unit; the pin
file alone is the gate).

## 5. Comment-ban gate (verbatim)

```text
$ python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/lb-build origin/main
comment-ban hits=0
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-multiarg-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The three refusals are measured verbatim on the release native (§1a SQL
        plus DataFrame plus ANSI context, §1c register plus read plus passing control)
        and on live Spark 4.1.2 in one JVM (§1b); each pin asserts the measured class
        and message fragment, and each pin is red when its refusal is absent (§2).
      artifacts: [python/repark/tests/test_v3_multiarg_1.py, python/repark/tests/v3_multiarg_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: The Spark cell is the verbatim live refusal, recorded by the committed
        `_record_v3_multiarg_1.py` driver (GAV from `_oracle_pins`, `--warehouse`, ivy
        from `REPARK_ORACLE_IVY`), which re-derives the cell and exits non-zero on drift.
      artifacts: [python/repark/tests/_record_v3_multiarg_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Refusal paths are the whole surface: SQL-door arity error, register-time
        metadata parse failure, Spark DDL refusal. No silent-answer path exists — §1c
        proves the foreign table refuses at register and the control passes.
      artifacts: [python/repark/tests/test_v3_multiarg_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; the unit adds no product code.
    - id: AT-5
      status: ATTACKED
      evidence: Memory catalogs under temp dirs; one short-lived JVM under jvm-lock.sh
        for the Spark cell only; REPARK_PARITY_LIVE unset for the pins; no network, no
        credentials.
      artifacts: [task/ledgers/staging/v3-multiarg-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every pinned value is a measured value (§1 verbatim blocks), not prose:
        the arity text, the untagged-enum fragment, the Spark first line.
      artifacts: [python/repark/tests/test_v3_multiarg_1.py]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: `git status` shows only the new pin files, the fixture, the registry
        row, the card, the ledger and three map.md entries; no Cargo.toml, lockfile,
        workflow or pin change.
      artifacts: [task/ledgers/staging/v3-multiarg-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The declaration lives in its registered home (registry row
        V3-MULTIARG-1) and the follow-up in its own card; the tests map, the mid-term
        map and the staging map carry the entries in the same commits.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/mid-term/v3-multiarg-1.md]
    - id: AT-10
      status: N/A
      justification: Single-round docs-plus-pin unit; no prior round to regress.
  complete: true
```

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 CONCLUDED with all
five clauses PROVEN, pins green, comment-ban hits=0.
