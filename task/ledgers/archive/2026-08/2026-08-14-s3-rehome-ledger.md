# Unit ledger — S-3 / Q1 `repark.sql` re-home

**Unit:** S-3 · **Date:** 2026-08-14 · **Lane:** repark · **Worktree:** `/tmp/grok-s3`
**Branch:** `grok/s3-rehome` · **Base freeze:** `d9a739123be8b00bc1fc1e6d4bbad875ba6caa76`
**SEPMO:** HIGH, octo + C4 · **Executor:** Grok (grok-4.5)
**JVM lock:** never taken.

**Charter:** `BRIEF-s3-rehome.md` + conductor-8 A4/A5/A6 + `docs/design/python-facade.md` §4 Q1
(design wins over BRIEF-s3 shorthand).

---

## §0 whole-or-HALT enumeration

The rename **can land whole**. Census of HALT traps:

| Trap | Finding | Verdict |
|---|---|---|
| Circular import (`repark/__init__` ↔ facade) | `__version__` bound first; `repark.spark` exports are PEP 562 lazy; internals import `repark.spark.<submodule>` not package attributes | landable |
| Packaging (`maturin python-source=src`) | `repark/spark/` is picked up automatically; no `pyproject.toml` metadata move required | landable |
| Census redirect cannot move | `compat/bootstrap.py` PATCH_MAP + imports retargeted to `repark.spark.*` | landable |
| `repark._native` / `repark.errors` / `__version__` | stay at top level (Q1 carve-outs) | landable |
| Frozen `_wire()` import paths | rewritten to `repark.spark.session` / `repark.spark.dataframe`; `test_t0_df_regions_import_freeze` follows | landable |
| Honest ANSI `repark.sql()` | `PyReparkSession.native()` = bare builder (`DataFusionDialect`, no SparkExtension). `repark-python → repark-sql` is a deliberate NON-edge + would edit `Cargo.lock` (forbidden tonight) | landable with residual |
| S-1 three files | not rewritten; `repark/functions.py` deprecation shim keeps `import repark.functions` collecting | landable (declared SQM conflict) |
| dbt-repark unmoved | not a HALT reason (A4) | n/a |

Never alias-only sql→spark: facade implementation **moved** under `repark.spark/`.
Never a lingering importable `repark.sql` module: `sql/` directory is gone.

**Residual (not HALT):** Iceberg-DDL `AnsiDialect` handlers are not wired from Python
tonight. `SELECT 1` and INT/INT truncation prove the native (non-Spark) door.

---

## What landed

1. Facade package at `python/repark/src/repark/spark/` (physical move).
2. Alias package at `repark.spark.sql` (same-object identity; sed `pyspark` → `repark.spark`).
3. `repark.sql(...)` ANSI-door callable in `repark/__init__.py`.
4. `import repark.sql` fails (package directory gone).
5. Top-level shim: `from repark import ReparkSession` (and facade names).
6. No native lazy DataFrame API.
7. A6: `docs/testing.md` row-2 paragraph flipped (that paragraph only).
8. Node-id map: [s3-rehome-node-id-map.txt](../../../s3-rehome-node-id-map.txt).
9. S-1 files **not** rewritten:
   - `python/repark/tests/test_decimal128_parity.py`
   - `python/repark/tests/test_sql_passthrough_parity.py`
   - `python/repark/tests/_record_decimal128_goldens.py`

---

## §6 handoff — release.md hard-blocker language (text only; do **not** edit release.md)

Morning landing / orchestrator release PR. Flip the Hard blockers bullet from
"`repark.sql` is still a module" to **resolved**:

> **`repark.sql` is no longer a module (resolved 2026-08-14, S-3).** The pyspark-alias
> package lives at `repark.spark.sql`; `repark.sql()` is the ANSI-door callable.
> Release-PR checklist: `python -c "import repark.sql"` exits non-zero, and
> `repark.sql("SELECT 1")` collects through the native door. The first tag may proceed
> on this item.

---

## Actor–Critic

**Context break executed; attacking artifacts, not memory.** Cycle 1+2 (floor S1).

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-S3-C1-1 | S2 | `repark.sql()` returns a facade `DataFrame` bound to a native session; Spark-shaped follow-on ops would be dishonest | `ACCEPTED_FLAGGED` — native lazy API is out of scope; pins use `to_arrow` only |
| F-S3-C1-2 | S2 | Native constructor uses `DataFusionDialect`, not `AnsiDialect` (no `repark-python → repark-sql` edge; lockfile ban) | `ACCEPTED_FLAGGED` — §0 residual; INT/INT pin proves non-Spark |
| F-S3-C1-3 | S3 | `repark/functions.py` deprecation shim exists so S-1 `import repark.functions` still collects | `ACCEPTED_FLAGGED` — declared SQM union; not a `repark.sql` module |
| F-S3-C2-1 | — | C2 re-attestation: no new ≥S1; facade 3049 passed; wheel `import repark.sql` exit 1 | converged |

max_cycles=4; converged at cycle 2. JVM lock never taken.

---

## Files not rewritten (S-1 / SQM union)

See §0. Orchestrator unions S-1 content + S-3 import lines at SQM.
