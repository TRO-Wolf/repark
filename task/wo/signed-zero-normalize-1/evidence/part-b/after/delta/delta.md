# SIGNED-ZERO-NORMALIZE-1 part B part 1: the verifier delta round (2026-11-13)

Both verifiers FAILED the head `b84adbc7` with no class (a) finding. This round answers items D1 to D8 of the delta
brief and nothing else. Builder: `claude-opus-5-5`. Spark is PySpark 4.1.2 with the Iceberg 1.11.0 runtime; RePark legs
ran on the earlier head package (product code `66fb7f13d`) and the base package (`276dc503`); the green runs are on
this round's head.

## D1. A `SET` that stops seat 2 is refused (verifier 1 F1, verifier 2 F2)

**Inventory** of DataFusion 54.1.0 configuration that can stop an optimizer rule, or the whole optimizer, from
running. Read from `datafusion-common-54.1.0/src/config.rs` (`OptimizerOptions`, 39 keys) and
`datafusion-optimizer-54.1.0/src/optimizer.rs` (`Optimizer::optimize`, the pass loop):

| key | effect on seat 2 | disposition |
|---|---|---|
| `datafusion.optimizer.max_passes` | the loop runs `while i < max_passes`; `0` runs no pass, so no rule runs (seat 2, the literal seat and every other RePark optimizer guard) | `0` refused; `1`, `3` and every other value accepted |
| `datafusion.optimizer.skip_failed_rules` | `true` keeps the previous plan when a rule errors and goes on; seat 2 has error paths (`comparisons`, `sort_key`, `named`), so a failing seat 2 would drop silently | `true` refused; `false` (the default) accepted |
| the 37 other `OptimizerOptions` keys (`enable_*`, `repartition_*`, `prefer_*`, `hash_join_*`, `filter_null_join_keys`, `expand_views_at_output`, `enable_unions_to_filter`, …) | each gates one built-in rule's behaviour or a physical choice; none removes a rule from the list or ends the loop. Seat 2 runs last in every pass whatever plan they leave | accepted |
| `SessionState::optimize`, `Explain` handling | no key; the optimizer always runs once per statement | — |

The analyzer has no pass limit or skip flag in 54.1.0. No repository test or document sets either refused value: the
only use is a historical attribution note, `docs/perf/cast-cost-2026-09-12.md`, which timed `max_passes` 0, 1 and 3
in September; its probe is not in the tree.

**Fix.** `df_guards::optimizer_seat_refusal` probes the value on a scratch `ConfigOptions` (so every spelling DataFusion
accepts, quoted or bare, any key case, is judged by DataFusion's own parse) and refuses `max_passes = 0` and
`skip_failed_rules = true` with `Error::Config`. It runs on the runtime `SET` path (`spill::maybe_apply_runtime_set`,
which every SQL statement on both doors passes, and which `spark.conf.set` reaches through its `SET`), now also for the
`SET <key> TO <value>` form, and on the builder config path (`session.rs`), beside the dead-key refusal.

**Pins.**
- Rust `key_tests.rs::a_set_statement_cannot_reach_the_door_policy`: `max_passes = 0`, `= '0'`, `TO 0`,
  `SET VARIABLE … = 0` and `skip_failed_rules = true` refused; builder config of both refused; controls
  `max_passes = 1`, `= 3`, `skip_failed_rules = false`, `batch_size = 4096` and builder `max_passes = 1` accepted;
  then the group and join answers. **Red** on the head's product code (`SET datafusion.optimizer.max_passes = 0 was
  accepted`), **green** after.
- Facade `test_signed_zero_keys_1.py::test_a_set_on_either_door_cannot_turn_the_key_wrap_off`: the same refused
  statements on the Spark door and native, `spark.conf.set` of both refused values, controls accepted on both doors
  (and `spark.conf.set(max_passes, 2)`), then the group (4) and self-join (18) answers on both doors. **Red** on
  `pkg-head` (`DID NOT RAISE`), **green** after.

## D2. `dropDuplicates(subset)` keeps the normalised key (verifier 2 F1)

**Measured** ([measure.py](measure.py) over [mkdata.py](mkdata.py)'s rows, which put `-0.0` and `-NaN` first;
legs [legs-spark.jsonl](legs-spark.jsonl), [legs-head-66fb7f13.jsonl](legs-head-66fb7f13.jsonl),
[legs-276dc503.jsonl](legs-276dc503.jsonl)). Spark 4.1.2, the same under `local[1]` and `local[4]`:

| variant | Spark 4.1.2 | head before this round | base `276dc503` |
|---|---|---|---|
| `dropDuplicates(['c'])` (DOUBLE), read | 3 rows; `c` is `0.0` and the canonical NaN; `f`, `k`, `v`, `i` are the kept row's own (`f = -0.0`, `f = -NaN`) | same rows, but `c` keeps `-0.0` and `-NaN` | 6 rows |
| `dropDuplicates(['f'])` (FLOAT) | `f` normalised, `c` keeps `-0.0` and `-NaN` | `f` keeps `-0.0`, `-NaN` | 6 rows |
| `dropDuplicates(['c', 'k'])` | 4 rows, both key columns normalised (payload NaN to canonical) | keys raw | 6 rows |
| `dropDuplicates(['k', 'v'])` (a DOUBLE and a string key) | 6 rows, `k` normalised | `k` raw (= base) | `k` raw |
| `drop_duplicates(['c'])` | as `dropDuplicates` | as `dropDuplicates` | 6 rows |
| `dropDuplicates()`, `distinct()` | normalised | EQUAL | 6 rows |
| `dropDuplicatesWithinWatermark(['c'])` on a batch frame | refused, `AnalysisException` | refused, same text | refused |
| `writeTo(...).create()` of the first, the FLOAT and the `(k, v)` forms | stores the normalised key | stores `-0.0` / `-NaN` | — |

Spark keeps the first row of each key; the head keeps the same rows (ids 1, 3, 6), so only the subset columns differ.

**Fix.** The facade's subset path (window `row_number`, then filter) ends with `subset_resolve._normalized_keys`, which
re-projects each FLOAT or DOUBLE subset column as `__repark_normalize_zero__(c) AS c` and every other column as its own
bound column. Non-float subsets return the frame unchanged. The call is reachable from the facade through one new arm of
the door-converged `call_scalar_expr` (`repark_core::extension::normalize_zero_call`). The UDF keeps the field's type and
nullability, so the schema is unchanged.

**Pins** (`test_signed_zero_keys_1.py`): `test_drop_duplicates_keeps_the_normalised_key_and_the_row_values` (four
subsets, `dropDuplicates` and `drop_duplicates`, every column's bits against Spark's, the column list and the schema),
`test_drop_duplicates_writes_the_normalised_key` (three `writeTo` targets, the subset bits and kept ids), and
`test_drop_duplicates_without_a_subset_and_distinct_match_spark` (control). **Red** on `pkg-head` (5 failures:
the four subsets and the write), **green** after.

## D3. `F.collect_set` and NaN payloads: branch (ii) (verifier 2 F3)

**Measured** (same legs; cells `cset-*`), groups over one file (`cs.parquet`), over four files (`cs4`), and after
`repartition(4)`, DataFrame and SQL, `local[1]` and `local[4]`:

| input | Spark 4.1.2, all variants | head DataFrame | head SQL, base both |
|---|---|---|---|
| two canonical NaNs | **2** elements | 1 | 1 |
| canonical NaN, `-NaN` | 2 | 1 | 2 |
| `-NaN`, `-NaN` | **2** | 1 | 1 |
| payloads 1 and 2, canonical NaN | 3 | 1 | 3 |
| `-0.0, 0.0` / `0.0, -0.0` | 1 zero, the first met; the sign changes with the file split and `repartition(4)` under `local[4]` | `[0.0]` | both zeros |
| `-0.0, 0.0, -NaN, NaN, NaN(p1), 1.5` | 5 | 3 | 6 |

Spark compares NaNs by IEEE inside `collect_set`: equal NaNs never merge, even with identical bits, while both zeros
merge. The element count does not depend on the plan; only the zero's sign does (already declared SIGN-ONLY).

**Branch (ii)** applies: Spark's rule is not "NaNs separated by their bits" (the base's behaviour), so no code change.
Added the declared registry row `SIGNED-ZERO-KEYS-1-COLLECTSET-NAN` beside `SIGNED-ZERO-KEYS-1-COLLECTSET`, the card
[COLLECTSET-NAN-ELEMENTS-1](../../../../../../roadmap/mid-term/collectset-nan-elements-1-card-2026-11-13.md) (S3) with its
row in `task/roadmap/mid-term/map.md` (paid by condensing the closed OFFSET-NESTED-SORT-1 and CROSS-JOIN-CONDITION-1
rows into one; baseline lowered from 151948 to 151927 bytes, 1007 lines), and the pin
`test_group_agg.py::test_collect_set_keeps_one_zero_and_its_sign_is_the_declared_residue` now also asserts RePark's
declared NaN answer: over `NaN, NaN(p1), -NaN, NaN, 1.5`, two elements, the canonical NaN and `1.5`. The COLLECTSET
row's "same with 1 or 2 partitions" is corrected: the zero's sign is plan-dependent.

## D4. OB-4's tie order (verifier 1 F2, verifier 2)

`same_rows` checks the OB-4 cells as the multiset of signed values plus the order of the normalised keys (`-0.0` read
as `0.0`), as SIGNED-ZERO-KEYS-1-TIES declares. OB-1, OB-2 and OB-3 stay ordered.

## D5. Facade pin gaps (verifier 2)

- **GB-8** (`GROUP BY g, d ORDER BY g, d`) is now checked in order. The NULL row is excluded from the order check (it
  stays in the multiset), because the native door places NULL last for ASC, a pre-existing difference both verifiers
  recorded (V-O4, F5); with it excluded the order is fixed.
- **NaN bits:** `test_nan_outputs_carry_the_canonical_bits_and_passthrough_keeps_the_payload` (Spark door and native)
  runs every group, distinct and set-operation cell whose truth holds a NaN and asserts each output NaN is
  `7ff8000000000000`; and on `nn_*` it asserts that `ORDER BY d, id` returns each NaN row with the bits a plain scan
  returns, at least one of them not canonical (`sqrt(-1)`, `-NaN`). Green on head; **red on the base package**
  (`NN-1|double`: `0x7ff8…`, `0xfff8…`), so it bites.

## D6. The countDistinct pin (verifier 1 F4)

The repository's convention for a pinned known divergence is a strict `xfail` whose reason names the unit or card
(`test_fnp_math_1.py`, `test_aws_acceptance_flat_matrix.py`). The pin is now
`test_count_distinct_over_two_columns_counts_signed_zero_pairs_as_one`: it asserts Spark's answer, 1 (PySpark 4.1.2,
measured 2026-11-13 over the pin's rows), under `xfail(strict=True)` whose reason says RePark's answer is wrong (3)
and names COUNT-DISTINCT-MULTI-SIGNED-ZERO-1 (records PR #1312). It XFAILs on head; a fix XPASSes and turns it red.

## D7. Records

The order's title and status line and the unit map's line no longer say "not built"; the registry rows
SIGNED-ZERO-KEYS-1 (the refused `SET`s, `dropDuplicates`, the new pins), SIGNED-ZERO-KEYS-1-COLLECTSET (the
plan-dependent sign), SIGNED-ZERO-KEYS-1-COLLECTSET-NAN (new) and SIGNED-ZERO-KEYS-1-TIES (the OB-4 check) are updated.

## D8. The final cost run (prepared, not run)

`cost3/` in the lane scratchpad (outside the repository): the runner rotates the build order by one each run
(run 1 `04d84ab9 head 04d84ab9-aa 276dc503`, run 2 `head 04d84ab9-aa 276dc503 04d84ab9`, …), keeps the four locks and
the 60 s load retry, maps `head` to the final head's package, and writes its raw legs and samples inside `cost3/`.
[../../cost/cost_probe.py](../../cost/cost_probe.py) gains case `f3`: a 10M-row `DOUBLE` table on a local hadoop
catalog written as 20 files with disjoint `d` ranges (`-0.0` only in the first million rows), joined against a
1000-row zero-free build side in the middle of the range (`count = 1000` on every build, guard (b)), five timed repeats,
plus one `EXPLAIN ANALYZE` that records the probe scan's lines and whether a dynamic filter reaches it. It gates at
the 1.20 bar, like F1 and F2.

## Gates (head `4442b88f1`; each run inside the lane's build wrapper)

| gate | exit | result |
|---|---|---|
| `cargo test --locked -p repark-core --lib --tests` | 0 | 1653 passed, 0 failed |
| `cargo test --locked -p repark-python --lib` (touched) | 0 | 176 passed |
| `cargo fmt --all -- --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `make py-test-facade FACADE_WORKERS=8` (ci) | 0 | 27913 passed, 711 skipped, 151 xfailed |
| `make py-test-facade FACADE_PROFILE=release FACADE_WORKERS=8` | 0 | 27918 passed, 706 skipped, 151 xfailed |
| `sync_map_md.py --check`, `check_map_md.sh`, `check_crate_dag.sh`, `check_manifest.sh` | 0 | |
| `check_docs_compaction.py`, `make check-map-size`, `make check-ledger-grammar`, `make check-docs-links` | 0 | |
| `typos@1.47.2`, `ruff@0.15.22 check .`, `ruff@0.15.22 format --check .` | 0 | |
| comment gate (`origin/main..HEAD`) | 0 | 0 hits |

`check_map_md.sh` is advisory: the maps at exact size baselines (`crates/repark-core/src/`, `session/`,
`df_guards/`, `python/repark/src/repark/spark/`, `spark/dataframe/`, `scripts/`) were not grown for this round's
one-line changes; the touched files are described in the maps below them and in this record.

The f3 trial on the earlier head package: `count = 1000`, and the probe scan line is
`IcebergTableScan projection:[d] predicate:[] N=16, metrics=[]` (no dynamic filter at the Iceberg scan).
