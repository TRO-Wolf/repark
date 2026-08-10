# G-5 — registry sweep ledger

**Date:** 2026-08-10 · **Branch:** `grok/g5-registry-sweep` · **Base:** `origin/main` @
`b5da280` (H-1d registry #35) · **Path:** STANDARD · **critic_engine:** `acc` (charter; octo /
overload disproportionate — mechanical acceptance rail) · **Brief:**
`planning/grok/BRIEF-g5-registry-sweep.md` (LocalRepark planning tree) + owner rulings 2026-08-10.

## Charter (frozen)

Move pre-registry disposed, pinned divergences into `docs/spark-sql-iceberg-parity.md` as rows;
inventory first; move-not-copy; zero engine-behavior change; no new live-tier scenarios; candidate
#1 carved out for H-1a split B.

## Inventory method

Wider search than the H-1d seed grep:

```text
grep -rn -E 'divergen|disclos|differs from Spark|unlike Spark|Spark would|DISCLOSED|KNOWN DIVERGENCE|DIVERGENCE-'
  --include='*.py' --include='*.rs' --include='*.md'
  python/repark/ crates/
```

High-signal marker pass (original H-1d spellings) enumerated separately for the seed queue.

**Blind spots (also stated in registry §1 Scope):** text search is not a semantic proof; no-pin
comments cannot become rows; polars-or-fork-only differences are out of Spark-registry scope;
DIVERGENCE-1 carved out for H-1a split B.

## Triage table

Disposition keys: **already-row** | **rowed-here** | **carve-out** | **ledger-only** | **not-divergence** | **comment-no-pin**.

| Hit / site | Kind | Disposition | Notes |
|---|---|---|---|
| Seed queue #1 `test_dogfood_gaps::test_divergence_timestamp_ltz_collect_passthrough` | DIVERGENCE-1 | **carve-out** | H-1a split B; not rowed |
| Seed queue #2/#3 `test_show_namespaces::test_show_namespaces_disclosed_divergences_fail_loud` | two refusals | **rowed-here** → NS-1, NS-2 | same pin, two arms |
| Seed queue #4 `test_catalog_surface::test_show_tables_in_not_implemented_divergence` | SHOW TABLES IN | **rowed-here** → ST-1 | |
| Seed queue #5 `test_catalog_surface::test_list_databases_location_uri_none_divergence` | locationUri None | **rowed-here** → FA-2 | |
| Seed queue #6 `test_interchange_parity::…int32_widens…` | int32→int64 | **rowed-here** → TY-4 | oracle: documented-value → H-2 G10 |
| Seed queue #7 `test_interchange_parity::…decimal_precision_widens…` | Decimal widen | **rowed-here** → TY-5 | oracle: documented-value → H-2 G10 |
| Seed queue #8 `test_errors::test_python_arg_errors_runtime_error_divergence_is_deliberate` | RuntimeError base | **rowed-here** → FA-3 | diagnostic-class / documented-semantics |
| Open (a) `test_metadata_tables::test_unpartitioned_partition_column_divergence` | Java Iceberg | **ledger-only** | not Spark; fork-side; one-line docstring clarify only |
| Open (b) `test_filter_predicate_rewrite::test_exact_duplicate_column_names_are_rejected_at_frame_construction` | construction refuse | **rowed-here** → ID-3 | pin asserts both paths |
| `test_union_distinct` int/string union | DISCLOSED | **already-row** TY-1 | live-mirror `int_union_string` |
| `test_union_distinct` inline decimal | DISCLOSED | **already-row** TY-3 | |
| `test_filter_predicate_rewrite` Column / double-quote bypasses | DISCLOSED | **already-row** ID-2 | live-mirror `filter_case_collision_bypasses` |
| `test_filter_predicate_rewrite` backtick span | DISCLOSED HOLE | **already-row** BL-2 | live-mirror `filter_backtick_identifier` |
| `test_errors` CAST execution pins | KNOWN DIVERGENCE F-BR-6 | **already-row** BL-1 | |
| `test_na_rename` / fillna nullability | disclosed | **already-row** TY-2 | live-mirror |
| `test_dropin_disclosure` lateral withColumns | disclosed | **already-row** FA-1 | |
| `test_parity_live` / `_live_parity` DISCLOSURES mirror | process | **already-row** | mirror gate; not a new divergence |
| `cross_door.rs` identifier case folding | DECLARED pin | **already-row** ID-1 | |
| MT-*/REF-*/DML-* pins in spark tests | seeded rows | **already-row** | §2 |
| `crates/repark-python/src/column.rs` zero-arg `concat` comment | KNOWN DIVERGENCE comment | **comment-no-pin** | no pin → cannot row; tracked todo-side |
| `test_polars_differential` `_divergence_*` | polars oracle | **not-divergence** | engine-vs-polars, not Spark |
| `test_group_agg` IEEE / count notes | Spark-aligned or accepted | **not-divergence** | not disposed Spark gaps |
| `test_fuzz_smoke` minimizer | tooling | **not-divergence** | |
| `test_ml_estimators_oracle` solver pin | ML solver string | **not-divergence** | not Spark SQL/Iceberg registry surface |
| `test_sql_passthrough_parity` divergence corpus | corpus of repark behavior | **not-divergence** / already-covered | CAST backlog points at BL-1 |
| `test_dynamic_flatten` residual | polars/engine | **not-divergence** | |
| `test_cache_persist::…out_divergence` | plan-sharing pin name | **not-divergence** | not a Spark disposition |
| `test_session_config_knobs` TIMING disclosure | builder timing | **not-divergence** for this sweep | drop-in §8 family / not queue seed |
| Module/map restatements of rowed sites | prose | reduced to links in this unit where they restated #2–#8 / open (b) | move-not-copy |
| Broader `divergen`/`disclos` prose in design/map docs | narrative | **not-divergence** | no new disposed pin |

**Counts (high-signal markers + seed + open items):**

| Bucket | Count |
|---|---|
| rowed-here | 8 (NS-1, NS-2, ST-1, FA-2, TY-4, TY-5, FA-3, ID-3) |
| carve-out | 1 (#1 DIVERGENCE-1) |
| ledger-only (Java Iceberg) | 1 |
| already-row (marker hits) | 10+ (seeded registry surface) |
| comment-no-pin | 1 (zero-arg concat) |
| not-divergence | remainder of looser-grep hits |

## Rows added (stable IDs)

| ID | Section | Pin | Oracle basis | live-mirror |
|---|---|---|---|---|
| NS-1 | §2.4 | `test_show_namespaces::…disclosed_divergences_fail_loud` (no IN/FROM arm) | documented (refusal) | no |
| NS-2 | §2.4 | same pin (nested arm) | documented (refusal) | no |
| ST-1 | §2.4 | `test_catalog_surface::test_show_tables_in_not_implemented_divergence` | documented (refusal) | no |
| ID-3 | §3 | `test_filter_predicate_rewrite::test_exact_duplicate_column_names_…` | documented | no |
| TY-4 | §4 | `test_interchange_parity::…int32_widens…` | documented-value → **H-2 G10** | no (G9/H-2 conversion candidate) |
| TY-5 | §4 | `test_interchange_parity::…decimal_precision_widens…` | documented-value → **H-2 G10** | no (G9/H-2 conversion candidate) |
| FA-2 | §5 | `test_catalog_surface::test_list_databases_location_uri_none_divergence` | documented | no |
| FA-3 | §5 | `test_errors::test_python_arg_errors_runtime_error_divergence_is_deliberate` | documented-semantics (diagnostic) | no |

## Open-item rulings (in-unit)

### (a) Java-Iceberg unpartitioned `partition` column

**Ruling:** not a registry row. Divergence from **Java Iceberg**, not Apache Spark. Disposition
recorded here only (REF-1 single-writer pattern: orchestrator files onward to the fork's
coordination surface). Allowed one-line docstring clarification applied:
"Java-Iceberg parity item, tracked fork-side — not a registry row." No STATUS entry; no registry
prose.

### (b) Exact duplicate column names at construction

**Ruling:** **row now** as ID-3. Pin asserts both construction paths refuse with
`unique expression names`; Spark half is documented (accepts construction, raises later on
reference). Exception path (queue) not needed.

## Pin strengthening

None required beyond existing strengthened pins (int32/decimal already assert Arrow + polars
dtypes). Message substrings on NS-1/NS-2/ST-1/ID-3 already match the row claims. No new test
functions or production code.

## False-claim findings

None. Every rowed pin's assertions match the docstring claim that moved into the row.

## G9 / H-2 conversion candidates (no scenarios built)

- TY-4, TY-5 → attach real oracle under **H-2 gap G10** (forced basis).
- No other new row carries a value claim needing a live mirror this unit.

## Move-not-copy sites

Pin docstrings / module notes reduced to registry links for NS-1/2, ST-1, FA-2/3, TY-4/5, ID-3;
`errors.py` warning → FA-3 note; `catalog.py` restatements → ST-1/FA-2 links; tests `map.md`
entries for those modules; Java-Iceberg clarify line only.

## Gates

| Gate | Result |
|---|---|
| `make ci` | green (2026-08-10) |
| `make py-test-facade` | green — 2531 passed, 44 skipped |
| mirror test (`test_disclosures_mirror_the_registry`) | green (inside facade suite) |
| `bash scripts/check_map_md.sh` | green |
| `python3 scripts/check_manifest.py` | green |

## ACC

| Phase | Verdict | Notes |
|---|---|---|
| Critic-1 Quality | NEEDS_REMEDIATION → remediated | Q-001..Q-007 (move-not-copy, ST-1 path, TY-5 Spark half, G10 note, NS-1 oracle prose) |
| Critic-2 Security | NEEDS_REMEDIATION → remediated | SAF-001..003 (ID-3 dual authority, FA-3/ST-1/FA-2 map restatements) |
| Re-spot after fix | residual greps clean; map+ruff green | no open ≥ S1 |

**Convergence label:** `ACC-CONVERGED` (after remediation).
