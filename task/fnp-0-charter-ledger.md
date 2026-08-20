# Unit ledger — FNP-0 charter · the Spark function parity campaign

**Unit:** FNP-0 (scope audit + approval gate) · **Date:** 2026-08-20 ·
**Executor:** Claude (Opus 5), orchestrator-side · **Base:** `454382f` (`main`, v0.5.0).
**Charter:** [docs/design/spark-function-parity.md](../docs/design/spark-function-parity.md) ·
**Slate:** [briefs/spark-function-parity.md](../briefs/spark-function-parity.md).
**SEPMO state:** `AGGRESSIVE_LOGIC_SCOPE_AUDIT` → `APPROVAL_GATE`. Severity floor **S1**.
No code unit opens until this ledger is fully `PROVEN` and the owner confirms (spine T3).

Owner decisions taken at kickoff (2026-08-20): one PR · lambda seam and SQL dialect in one unit ·
"Rust-owned" means owning the semantics · coverage target is everything reachable without a JVM ·
the four sub-project families declared absent-and-loud, collation and crypto built (D-7).

## Evidence attached to this gate

| Artifact | What it proves |
|---|---|
| [fnp-0-census/](fnp-0-census/map.md) | The nine-agent read-only census: facade classification (345 rows), the PySpark gap partition, the lambda machinery spec, the kernel ownership map |
| Spike A (2026-08-20, reverted) | The expr-API lambda path executes: `transform(arr, x -> x+1)` on `[1,2,3]`/`[10,20]` → `[2,3,4]`/`[11,21]`, no parser involved |
| Spike B (2026-08-20, reverted) | A session-wide `sql_parser.dialect = Databricks` fails **8** of 1,984 workspace tests — 2 in `repark-core` (struct literals), **6 in `repark-sql/tests/cross_door.rs`** |
| Spike C (2026-08-19, reverted) | With the dialect set, `array_transform` / `array_filter` / `array_any_match` and the aliased Spark spellings `transform` / `filter` / `exists` all execute correctly through SQL |

## Proposition ledger

Every clause carries exactly one verdict. The gate passes iff zero `OPEN`, zero `REJECTED`, and
the owner confirms.

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Every name in `repark.spark.functions.__all__` reaches the engine through a Rust-owned expression: no SQL text construction, no composition of two or more engine calls, no branch on argument type/arity/value selecting a different engine expression. | Domain enumerated: the 333 exported names classified in the census. 55 are non-compliant and named in design §4.4; 5 are classes/decorators and out of scope; the remaining 273 are compliant today. Sanctioned exception: `udf`, `pandas_udf`. Per-name compliance becomes machine-checkable by a guard script in FNP-8. | **PROVEN** (enumeration complete) |
| C-002 | No facade function performs row-level computation in Python. | Census `PY_COMPUTE` bucket = 2, both the sanctioned UDF path. The existing AGENTS.md rule holds today; FNP-8 must not break it. | **PROVEN** |
| C-003 | The eleven Spark higher-order functions are exported and evaluate correctly through the Column entry point with a Python lambda. | Domain: 11 functions × each arity Spark accepts, enumerated per design §3.5 — including the `(element, index)` forms of `transform`/`filter`, `aggregate`'s optional finish lambda, and `map_zip_with`'s ternary. Each element pinned on the Arrow path, value AND type. Mechanism proven by Spike A. | **PROVEN** |
| C-004 | The same eleven evaluate correctly through the Spark SQL door and `F.expr` with `x -> y` syntax. | Same enumeration, per entry point, per the entry-point matrix. Mechanism proven by Spike C. | **PROVEN** |
| C-005 | Naming the Spark dialect at the Spark door changes no native-door behaviour. | The native-door suites pass unchanged and `repark-sql/tests/cross_door.rs` passes in full. Baseline measured by Spike B: a session-wide flip fails 8, six of them cross-door. | **PROVEN** |
| C-006 | Every Spark-door behaviour the dialect change moves is either fixed or carries a divergence-registry section. | Spark-door suites + facade cohort run against the change; every delta dispositioned in the FNP-4 ledger. Registry row BL-2 re-checked in the same unit. | **PROVEN** |
| C-007 | Every PySpark 4.1.2 name reachable without a JVM is exported and evaluates correctly. | Domain: the 506 names partitioned into REACHABLE (500) and UNREACHABLE (6). Closed by owner ruling **D-7** (2026-08-20): the four sub-project families — sketches (32), CSV/XML/XPath (11), VARIANT (8), geospatial (5) — are **declared absent-and-loud** under the G15 pattern, and **collation (2) and crypto/masking (4) are IN SCOPE and built** (G15 opens; a cipher crate is accepted). Actionable target: **160** names. | **PROVEN** |
| C-008 | Every unreachable name is exported and raises `UnsupportedOperationException` naming its divergence-registry section. | Enumerated: `java_method`, `reflect`, `try_reflect`, `unwrap_udt` (JVM reflection / UDT registry), plus `input_file_block_start` / `input_file_block_length` (Spark's `InputFileBlockHolder` thread-local, no DataFusion surface). Each claim states its mechanism, never the word "JVM" alone. | **PROVEN** |
| C-009 | Nothing is silently absent: `set(pyspark.sql.functions.__all__) - set(repark.spark.functions.__all__)` is empty. | One assertion in the facade suite. Gap today measured at 181 absent + 35 raising + 7 conditional = 216 of 506. | **PROVEN** |
| C-010 | Structure gates stay green without a raised ceiling. | `make ci` green. `check_rust_file_size` / `check_lib_py` EXCEPTIONS ratchet down or hold. `function_dispatch.rs` (906 of a 1500 default) splits into `column/dispatch/`; the four `#[path]` module inclusions in `repark-functions` convert to the canonical tree. | **PROVEN** |
| C-011 | The delivered state is recorded where the project records state. | STATUS.md carries the re-run census numbers; `task/map.md` links every unit ledger; the divergence registry carries every new section. Owned by FNP-Z. | **PROVEN** |
| C-012 | The facade and the SQL door resolve the same kernel for every name reachable from both. | Domain enumerated by the ownership map: 19 names diverge today — 17 latent, plus `to_timestamp` (facade bypasses `SparkToTimestamp`, so TZ-4 typing and session-zone localization are lost) and `avg` (facade bypasses `SparkAvgWithRetract`). Each closed, or registered with the divergence stated. Extends the policy FN-GT1/GT2 already applied to sixteen names. | **PROVEN** |

`LOGIC_SCORE` = **12/12 `PROVEN`** — zero `OPEN`, zero `REJECTED`.

**GATE PASSED, 2026-08-20.** The owner answered the C-007 question (ruling D-7, design §8) and
confirmed explicitly: *"Approve — open FNP-1."* The charter is **frozen** (spine T3 →
`PRE_EXECUTION_REVIEW`). Changing it now requires a new pass through the audit, not an in-place
edit.

## Vigilance notes (Invariant V, standing from gate-pass)

- **The 55/150 correction.** An early hand classification put the repatriation target near 150.
  The census's finer read gives 55. The larger figure was never load-bearing but is recorded here
  so no downstream document inherits it.
- **`__all__` membership overstates the surface.** 333 exported, 42 of them raising. Any future
  claim phrased as "RePark has N functions" must say which N.
- **The unreachable partition is the cheap way out.** Every UNREACHABLE claim carries a mechanism.
  Growth of that set during execution is a drift alarm (T8), not a routine reclassification.

## Disk (AGENTS.md "Resource discipline")

Checked before each spike: 448 GB free of 1.8 TB (75% used), `target/` at 60 GB. Two spikes built
incrementally against the warm target directory; no worktree created; no artifacts retained. Both
spikes reverted from backups and `git status` verified clean after each.
