# Unit ledger — H-1a **split A**: session-timezone conf surface, oracle override, differential rows

**Unit:** H-1a of the V2 Engine Hardening campaign
([../briefs/v2-engine-hardening.md](../briefs/v2-engine-hardening.md) "H-1a", decision D7 in
[../docs/design/v2-engine-hardening.md](../docs/design/v2-engine-hardening.md)) ·
**Date:** 2026-08-10 · **Panel:** FULL (semantics + oracle lenses)

**This ledger covers SPLIT A only.** The brief's split rule reads: *"unit A = the conf surface +
the registry override + the differential rows; unit B = the extraction fix + its Rust pins.
Neither half lands without its own tests."* Split A therefore changes **no evaluated result**: it
adds a validated session-timezone configuration, teaches the live oracle registry to run a
scenario under a non-UTC session, and records the divergence class against real PySpark. The
"§ Split B (reserved)" section at the end is deliberately empty.

---

## 1. What landed

### 1.1 The configuration surface (one key, resolved once, engine-validated)

| Layer | File | What it holds |
|---|---|---|
| Engine | `crates/repark-core/src/session_time_zone.rs` | `SESSION_TIME_ZONE_KEY` (the ONE spelling), `DEFAULT_SESSION_TIME_ZONE = "UTC"`, the validated `SessionTimeZone` type, `resolve_session_time_zone` |
| Engine | `crates/repark-core/src/session.rs` | `build()` resolves the zone **once** and stores it; `ReparkSession::session_time_zone()` reads it back |
| Facade | `python/repark/src/repark/session/session_time_zone.py` | the same key spelling, the `UTC` default, `warn_runtime_session_time_zone_not_applied` |
| Facade | `_funcs.py` / `builder_conf.py` / `session_core.py` | the `conf.get` default; `conf.set`/`conf.unset` accepted-warned-not-stored (D-A5); exclusion from the `getOrCreate` reuse fold |

`ReparkSession.builder.config("spark.sql.session.timeZone", "America/New_York")` → the facade
passes the whole `.config` map to the binding → `ReparkSessionBuilder::configs` → `build()`
validates the zone against Arrow's zone database and stores it. An unknown zone is
`Error::Config` (→ `IllegalArgumentException`) **at `getOrCreate`**, never at query time.

### 1.2 The live-registry per-scenario session-conf override

`Scenario.session_conf` (a tuple of pairs — the dataclass is frozen) applied by each engine's own
mechanism: the shared JVM oracle takes it through `spark_session_conf` (set around the leg,
restore after); repark takes it by BUILDING a session with it, because repark resolves the zone
once at construction. Two scenarios added, one either side of UTC. Size pin **27 → 29** and the
uniqueness pin moved in the same diff, plus a new pin that the non-UTC overrides exist and use the
one key spelling. `docs/port/census.md`'s record of that count moved with them.

### 1.3 The differential rows (record mode, real PySpark 4.1.2)

`python/repark/tests/test_session_timezone_parity.py` — 20 recorded recipes + the
`current_timestamp` type row: **14 for gap G1** (budget 10–14) and **7 for gap G16** (budget 6–8).
18 of the 20 run over scalar literals; the 2 `column_extract_*` rows run over a real tz-aware
TIMESTAMP **column** (a two-row in-memory frame registered as a temp view) under each non-UTC
zone — the brief's own recipe wording, added in the fix pass (§8, finding O-2). Every Spark half
was recorded by running the committed recipe on live PySpark; nothing is hand-computed, and the
driver that records them is committed (`_record_session_timezone_goldens.py`).

---

## 2. Decisions, with rationale

**D-A1 — The engine owns the key and the validation; the facade owns no second validator.**
Two validators drift. The facade forwards the value and surfaces the engine's refusal. Cost,
accepted and recorded: on the `getOrCreate` **reuse** path no new session is built, so an invalid
zone is neither validated nor applied — it falls into the existing engine-knob `unapplied` warning
("engine knobs are fixed at session build"). Nothing is silently wrong; the value simply does not
take, loudly. (Contrast `repark.memory.limit.gb`, whose *range* check is facade-side and therefore
does fire on reuse. Zone-id validity is not a range check — it needs the zone database.)

**D-A2 — The resolved zone lives on `ReparkSession` (repark-core), not in a DataFusion
`ConfigExtension`.** Three candidate homes were considered:

1. a `ConfigExtension` in `repark-functions` (the `repark.sql.maxArrayElements` precedent),
2. DataFusion's own `datafusion.execution.time_zone`,
3. a typed field on the session, resolved in `build()`.

(2) was rejected outright: DataFusion's `now()` reads that option, so setting it would change
`current_timestamp` **in this unit**, which is exactly the evaluated-result change split A must
not make. (1) was rejected for this split because `extensions_options!` derives a *settable*
option path (`SET <prefix>.session_time_zone = …`), which is a second, live spelling of the knob —
and the unit's acceptance gate is "exactly one authoritative spelling". (3) keeps the value
immutable for the session's life and needs no new crate edge: `repark-core` is tier 2 and
`repark-functions` is a tier-3 leaf with no internal deps, so a core→functions edge would be a
forbidden upward edge under `scripts/check_crate_dag.py`. **Consequence handed to split B:** it
must choose how the zone reaches the extractor layer — the natural seam is `SparkExtension`
(which already sees the builder conf map and depends on both crates) constructing zone-aware
shims, or a `ConfigExtension` installed from that same hook. Split B owns that decision; nothing
here forecloses it.

**D-A3 — The default is `UTC`, not the host's local zone.** Spark defaults to the JVM's local
zone. Matching that would (a) make a run's results depend on the host and (b) require a
host-environment read, which `docs/adr/0004-server-prep-disciplines.md` forbids. Declared as a
divergence with a registry row (§6) rather than inherited silently.

**D-A4 — Exactly one spelling, no alternates.** The neighbouring knobs in this repo deliberately
carry two spellings (`repark.sql.maxArrayElements` / `…max_array_elements`;
`repark.batch.size` / `spark.sql.execution.arrow.maxRecordsPerBatch`). This one does not, because
the acceptance gate asks for one and because the Spark spelling has no repark-native counterpart
worth inventing. A lookalike is an unknown `.config` key and configures nothing — pinned by
`lookalike_spellings_are_not_a_second_way_to_set_the_zone` over six near-misses.

**D-A5 — A runtime `spark.conf.set` of the key is ACCEPTED, warned once, and NOT stored.**
*This decision was reversed mid-unit by evidence; both halves are recorded because the reversal is
the useful part.*

*First choice (wrong):* refuse loud, copying the memory-pool knob's "one truth, not two lying
knobs" idiom — `IllegalArgumentException`, build-time only.

*What killed it:* the full facade suite went red on
`test_pyspark_compat_smoke.py::test_compat_smoke_suite_in_subprocess`, whose subprocess runs the
**pinned Apache test**
`pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_from_pandas_with_dst`.
That test sets this exact key through PySpark's own `sql_conf` context manager
(`.../test_creation.py:139`). A raise there is not a strictness win — it is a drop-in regression on
a *pinned* test, i.e. census movement, on a promise ("change the import line") that outranks the
idiom.

*Final choice:* the `.master(...)` / arrow-batch-sentinel shape (OTH-010) — accept the call, emit
the disclosure once per process, and **do not store the value**, so `conf.get` keeps reporting the
zone the live engine session actually has. `conf.unset` of the key is handled the same way, because
tombstoning it would let `conf.get` fall back to the `UTC` default on a session built with another
zone — the same split-brain by another route. The facade therefore never lies; it declines,
audibly. Declared divergence, registry row in §6.

*Handed to split B:* if the fix routes the zone through DataFusion `ConfigOptions` (see D-A2), a
runtime `conf.set` becomes *implementable* — forward it as a live `SET` and the divergence
disappears entirely. That is a better end state than either option here, and it is split B's to
take or decline deliberately.

**D-A6 — THE ORDERING QUESTION: divergent rows land as explicit current-behavior DISCLOSURES.**
The extraction fix is split B, so `repark == Spark` rows would be red on arrival, and a red gate is
not acceptable. Deleting the rows until B lands would leave the CRITICAL class unmeasured. The
harness's own idiom for this is the live registry's `Disclosure` (pin BOTH halves; a silent
convergence reds), and the facade tree's `test_divergence_*` convention (pin repark's behavior,
record Spark's half as a constant, and say in the message what must be revisited if it changes).
This unit follows both: each divergent row carries repark's actual Arrow output **and** the
recorded Spark output, asserts repark still matches its own half, and asserts the two **still
differ**, naming the in-flight fix. When split B lands, each row flips `repark=None` — and that
flip is precisely the revert-red evidence the testing contract demands, because reverting the fix
turns the equality assertion red again.

*Structural improvement taken over the plain disclosure pattern:* an all-disclosure corpus cannot
distinguish a zone-aware engine from a zone-blind one — every row would still "diverge" if a fix
moved results the WRONG way. So the corpus carries **two control rows** that assert plain
EQUALITY today and must stay equal after the fix: DATE extraction and leap-day DATE arithmetic are
session-zone independent by contract. A fix that pushed the session zone into the DATE path reds
there. `test_session_timezone_row_set_covers_both_gap_budgets` asserts at least one equality row
exists, so the corpus cannot degenerate into all-disclosures later.

**D-A7 — The two live scenarios assert EQUALITY, and are honest about why.** A live scenario
asserts `repark == pinned golden == live Spark`; a timestamp-extraction scenario would therefore
red the live tier today. The two added scenarios pin a real invariant instead — DATE extraction and
DATE arithmetic must NOT move with the session zone — under a non-UTC ORACLE session. They prove
the override reaches the oracle end to end, and they are the pins that catch a zone-blind fix that
over-reaches into the DATE path. The divergent TIMESTAMP rows for the same class live in the
facade corpus as recorded disclosures until split B; converting them into live disclosures is a
natural split-B follow-on.

---

## 3. Evidence per acceptance-gate item

The brief's H-1a gate is written for the WHOLE unit; the items below are the ones split A can
discharge. Items belonging to split B are named as such, not silently claimed.

| Gate item | Verdict | Evidence |
|---|---|---|
| A session-conf grep shows exactly ONE authoritative spelling of the timezone key | **MET** | §4 grep; `lookalike_spellings_are_not_a_second_way_to_set_the_zone` (Rust) + `test_registry_runs_at_least_two_scenarios_under_a_non_utc_oracle` (asserts every override uses the one key) |
| At least two live-tier scenarios run under a non-UTC oracle | **MET** | `date_extractor_under_new_york_session`, `date_math_under_tokyo_session`; full live run green (§5) |
| With the live gate unset they SKIP with a visible reason, never a silent pass | **MET** | JVM-free run: 32 passed, 33 skipped, reason `REPARK_PARITY_LIVE unset …` (§5) |
| The registry's size pin and its name-uniqueness pin are updated in the same diff as the registry change | **MET** | `test_registry_covers_the_mandated_golden_family` 27 → 29 for both pins, same file, same diff; `docs/port/census.md` count moved too |
| 10–14 differential rows vs real PySpark (record mode) for G1 | **MET (14)** | `G1_ROWS` (13 = 11 scalar-literal + 2 column-path) + `test_current_timestamp_type_and_zone_disclosure` (1), budget pinned by `test_session_timezone_row_set_covers_both_gap_budgets` |
| Fold in gap G16: 6–8 additional differential rows | **MET (7)** | `G16_ROWS` |
| Rows assert on the Arrow path, value AND type | **MET** | every row goes through `repark_parity.assert_frames_equal`, whose signature is `(name, type, nullable)` per field, then bit-exact values; no `show` anywhere |
| Every class pinned at all FOUR entry points (native DataFrame / ANSI door / Spark door / facade) | **CLAIMED AS SPLIT B — NOT MET HERE** | Split A pins ONE cell: the facade `sql()` door. It now pins it over a tz-aware timestamp COLUMN as well as over scalar literals (`column_extract_under_new_york_session` / `..._tokyo_session`), so the brief's literal recipe is constructible and constructed — but three of the four cells (native DataFrame API, ANSI door, Spark door) are **unpinned in this split**, deliberately and by declaration, not by silence. The original justification ("until extraction honors the zone the other cells would pin the same absence again") does NOT survive review: pinning an absence per cell is exactly what this corpus does, and the panel demonstrated the DataFrame-API cell diverges identically today. The honest statement is: split B owes the matrix, and owes it WITH the fix. Residual §7.1. |
| The brief's recipe: `year`/`month`/`day`/`hour` over a tz-aware timestamp COLUMN under two non-UTC session zones | **MET** | `column_extract_under_new_york_session` (all four fields move, incl. the YEAR across a boundary) + `column_extract_under_tokyo_session` (only the hour moves — the pair is what distinguishes a session-zone bug from an offset-sign bug); both re-derived against live PySpark 4.1.2 by the committed record driver. Enforced by `test_session_timezone_row_set_covers_both_gap_budgets`, which asserts the column family spans BOTH zones. |
| Reverting the extraction change reds at least one pin per named class | **SPLIT B** | there is no extraction change in split A; the flip of each disclosure row to equality IS that evidence, and it happens in split B |

---

## 4. Gate output (verbatim)

### 4.1 `make ci`

```text
$ make ci
cargo fmt --check
cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods
cargo clippy --locked --workspace --lib --bins --exclude repark-python -- \
        -D clippy::disallowed_methods -D clippy::unwrap_used -D clippy::expect_used ...
cargo clippy --locked -p repark-python --lib -- \
        -D clippy::disallowed_methods -D clippy::unwrap_used -D clippy::expect_used ...
crate-dag: 20 internal edges clean (4 dev, 15 normal, 1 optional) across 9 of 9 mapped crates
lib-rs: 9 crate roots clean (no inline test modules; ceilings held)
lib-py: 54 files clean (ceilings held; no-stub rule held)
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
          index, the status document and the crate maps
cargo check --locked --workspace
uvx ruff@0.15.22 check .
All checks passed!
uvx ruff@0.15.22 format --check .
240 files already formatted
uv lock --locked
Resolved 29 packages in 1ms
uvx taplo@0.9.3 format --check
uvx taplo@0.9.3 lint
uvx typos@1.47.2
EXIT=0

NOTE (honest): the FIRST `make ci` run of this unit was RED at `py-lint` — two untracked scratch
scripts (the probe and the record-mode driver) were sitting in the worktree root and ruff lints the
whole tree. They were moved OUT of the worktree (they must never land); a second run was RED once
more on `UP017` (`dt.timezone.utc` -> `dt.UTC`) in the new test file; the third and final run is the
one above. The gate did its job twice.
```

### 4.2 `make test` (Rust workspace)

```text
$ make test          # cargo test --locked --workspace
...
test session_time_zone::tests::absent_key_resolves_to_the_utc_default ... ok
test session_time_zone::tests::iana_zone_id_is_accepted_verbatim ... ok
test session_time_zone::tests::fixed_offset_is_accepted ... ok
test session_time_zone::tests::padded_value_is_trimmed_not_treated_as_a_different_zone ... ok
test session_time_zone::tests::unknown_zone_fails_loud_naming_the_key ... ok
test session_time_zone::tests::blank_value_fails_loud_rather_than_falling_back_to_the_default ... ok
test session_time_zone::tests::lookalike_spellings_are_not_a_second_way_to_set_the_zone ... ok
test session_time_zone::tests::bare_session_carries_the_utc_default ... ok
test session_time_zone::tests::builder_conf_reaches_the_built_session ... ok
test session_time_zone::tests::invalid_zone_fails_the_build_not_a_later_query ... ok
test session_time_zone::tests::session_clone_shares_the_resolved_zone ... ok
...
31 test binaries, every one `test result: ok`; 0 `test result: FAILED`
EXIT=0
```

### 4.3 Facade suite

The numbers below are the FIX-PASS re-capture (2026-08-10, post-§8). The actor's original
capture recorded `2593 passed, 36 skipped`; BOTH panel lenses re-ran the identical command on the
identical tree and got `2594 passed, 36 skipped`, twice each. The one-test gap was never
reproducible and is not environment-sensitive — collection is deterministic from the file tree —
so the honest reading is that the actor's number was captured mid-edit, one test before the final
tree. It is corrected here rather than explained away. The fix pass then added 6 tests (2 column
rows + 4 conf-surface pins), which is the whole of the 2594 → 2600 move.

```text
$ PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark/tests -q
2600 passed, 36 skipped, 46 warnings in 142.79s (0:02:22)
EXIT=0

Run against the parity-live provisioning (the four facade extras PLUS `--extra record`), NOT a bare
`make py-test-facade` sync — deliberately, because `uv sync` is EXACT and the facade-extras-only
sync uninstalls pyspark, which would have silently skipped the Apache compat-smoke subprocess that
found the D-A5 regression. `make py-test-facade` verbatim was run separately (below) once the
recording work no longer needed pyspark.

$ make py-test-facade
uv sync --locked --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark
cd python/repark && VIRTUAL_ENV=... uvx maturin@1.14.1 develop
PYTHONPATH=python/repark-parity/src ... uv run --no-project python -m pytest python/repark/tests -q
2564 passed, 46 skipped, 37 warnings in 91.47s (0:01:31)
EXIT=0

The denominator differs from the run above (2564/46 vs 2600/36) for a known reason and not a
regression: this target's `uv sync` is EXACT and does NOT request `--extra record`, so pyspark is
uninstalled and the pyspark-oracle cohort (compat smoke, the UDF/pandas oracle files) skips. That is
the target's normal shape; it is why the recording work above ran against the parity-live
provisioning instead.
```

### 4.4 `bash scripts/check_map_md.sh`

```text
$ bash scripts/check_map_md.sh
map_md EXIT=0
(silent on success; the guard reads the STAGED set, so every touched directory's map.md was staged
alongside its code — crates/repark-core/src, crates/repark-core/src/session_time_zone (NEW dir, new
map.md with a populated ## Debug), python/repark/src/repark/session, python/repark/tests, task)
```

### 4.5 `python3 scripts/check_manifest.py`

```text
$ python3 scripts/check_manifest.py
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc index,
the status document and the crate maps
manifest EXIT=0
(run because docs/port/census.md — a declared document — was edited; no manifest field changed)
```

### 4.6 Record mode against live PySpark 4.1.2 (the oracle leg)

```text
$ JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python <record driver>
[G1] year_of_instant_under_new_york_session [America/New_York] PASS
    live spark schema = [('year_part', 'int32', True)]
    live spark rows   = [{'year_part': 2023}]
[G1] month_of_instant_under_new_york_session [America/New_York] PASS
[G1] day_of_instant_under_new_york_session [America/New_York] PASS
[G1] hour_of_instant_under_new_york_session [America/New_York] PASS
    live spark rows   = [{'hour_part': 8}]
[G1] hour_of_instant_under_tokyo_session [Asia/Tokyo] PASS
    live spark rows   = [{'hour_part': 21}]
[G1] year_month_day_of_instant_under_tokyo_session [Asia/Tokyo] PASS
[G1] to_timestamp_of_zone_suffixed_string [America/New_York] PASS
    live spark schema = [('ts', 'timestamp[us, tz=UTC]', True)]
[G1] dst_spring_forward_instant_hour [America/New_York] PASS
[G1] dst_fall_back_repeated_local_hour [America/New_York] PASS
    live spark rows   = [{'before_part': 1, 'after_part': 1}]
[G1] tz_aware_to_naive_round_trip [America/New_York] PASS
[G1] date_trunc_day_across_a_zone_boundary [America/New_York] PASS
    live spark rows   = [{'day_start': datetime.datetime(2024, 6, 14, 4, 0, tzinfo=ZoneInfo('UTC'))}]
[G16] pre_1970_extract_under_new_york_session [America/New_York] PASS
[G16] pre_1970_timestamp_cast_to_bigint [America/New_York] PASS
    live spark rows   = [{'epoch_value': -1800}]
[G16] year_boundary_date_trunc_under_tokyo_session [Asia/Tokyo] PASS
[G16] year_boundary_extract_and_format_under_new_york_session [America/New_York] PASS
[G16] leap_day_extract_under_new_york_session [America/New_York] PASS
[G16] date_extraction_is_session_zone_independent [America/New_York] PASS
[G16] leap_day_date_arithmetic_is_session_zone_independent [Asia/Tokyo] PASS
[G1] current_timestamp live type = timestamp[us, tz=UTC], nullable=False

record mode: 18 rows re-derived, 0 mismatch(es)
```

**Re-captured in the fix pass**, after the driver was committed and the two column rows added:

```text
$ JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONPATH=python/repark-parity/src \
    .venv/bin/python python/repark/tests/_record_session_timezone_goldens.py
...
[G1] column_extract_under_new_york_session [America/New_York] PASS
[G1] column_extract_under_tokyo_session [Asia/Tokyo] PASS
...
[G1] current_timestamp live type = timestamp[us, tz=UTC], nullable=False

record mode: 20 rows re-derived, 0 mismatch(es)
EXIT=0
```

The driver imports `ROWS` from the COMMITTED test module and runs each row through the SAME
`run_row` helper the assertions use, so the recorded golden and the asserted recipe cannot drift
apart — there is one recipe, not two copies. It is now COMMITTED
(`python/repark/tests/_record_session_timezone_goldens.py`, §8 finding O-4): the actor left it as
scratch outside the worktree, which made "recorded against live PySpark 4.1.2" unfalsifiable from
inside the repo the moment that session ended. It never edits the corpus — it prints the live
values and exits non-zero, because a driver that rewrites its own oracle launders drift.

### 4.7 The live oracle tier, armed

```text
$ JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 \
    PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark/tests/test_parity_live.py -q
66 passed in 17.54s          # fix-pass re-capture; was 65 before §8 added the override pin
  (29 routine JVM-free scenario legs + 29 live triple legs + 4 live disclosure legs + the flag
   detector + the two registry pins + the `build_repark_engine` override pin added in §8; the two
   non-UTC scenarios assert repark == pinned golden == live Spark under America/New_York and
   Asia/Tokyo respectively)

$ (same command with the gate UNSET)
33 passed, 33 skipped in 0.79s   # was 32/33; the new override pin is JVM-free
  skip reason: "REPARK_PARITY_LIVE unset — the live PySpark oracle tier is skipped (routine CI is
  JVM-free). Set REPARK_PARITY_LIVE=1 with a JVM present (...) to run it." 
```

---

## 5. Provocation proofs

This unit adds no new *mechanical gate* (no lint entry, guard script, CI step or hook), so
`docs/testing.md` "Gate provocation proofs" does not bind it. Three of its claims are nonetheless
detection claims, and a green run proves nothing about detection — so each was provoked both ways.
**None of the provocations is committed**; each was reverted immediately, and the clean run is
captured beside the red one.

### P-1 — "exactly one spelling" (Rust)

*Claim:* a lookalike conf key is not a second way to set the session zone.

**must-FAIL** — plant an all-lowercase alias in `resolve_session_time_zone`
(`.or_else(|| config.get("spark.sql.session.timezone"))`):

```text
$ cargo test --locked -p repark-core session_time_zone
test session_time_zone::tests::lookalike_spellings_are_not_a_second_way_to_set_the_zone ... FAILED

---- session_time_zone::tests::lookalike_spellings_are_not_a_second_way_to_set_the_zone stdout ----
thread '...' panicked at crates/repark-core/src/session_time_zone/tests.rs:98:9:
assertion `left == right` failed: "spark.sql.session.timezone" must not be a second spelling of
spark.sql.session.timeZone
  left: "America/New_York"
 right: "UTC"

test result: FAILED. 10 passed; 1 failed; 0 ignored; 0 measured; 90 filtered out
error: test failed, to rerun pass `-p repark-core --lib`
```

**must-PASS** — alias reverted:

```text
$ cargo test --locked -p repark-core session_time_zone
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 90 filtered out; finished in 0.01s
```

### P-2 — the disclosure-convergence detector (Python)

*Claim:* if repark and Spark ever agree on a disclosed row, the row goes RED instead of passing
quietly.

**must-FAIL** — simulate convergence by moving the recorded Spark golden of
`year_of_instant_under_new_york_session` to repark's value (2023 -> 2024):

```text
$ pytest "...::test_session_timezone_row_matches_spark_or_still_diverges[year_of_instant_under_new_york_session]" -q
>       assert _frames_differ(row.repark, row.spark), (
E       AssertionError: year_of_instant_under_new_york_session: repark and Spark have CONVERGED —
E       this disclosure is stale. Do not delete the row: flip it to an equality row (repark=None)
E       and record the convergence. the instant is 2023-12-31 23:30 in New York, so Spark's year is
E       2023; repark extracts in the stored (UTC) zone and answers 2024. Flipped to equality by the
E       session-timezone extraction fix (briefs/v2-engine-hardening.md, H-1a split B).
E       assert False
E        +  where False = _frames_differ(pyarrow.Table\nyear_part: int32\n----\nyear_part: [[2024]],
E                                        pyarrow.Table\nyear_part: int32\n----\nyear_part: [[2024]])
1 failed in 0.44s
```

**must-PASS** — golden restored:

```text
$ pytest "...[year_of_instant_under_new_york_session]" -q
1 passed in 0.39s
```

### P-3 — the non-UTC-oracle registry pin (Python)

*Claim:* the registry cannot silently lose its ability to run the oracle in a non-UTC session.

**must-FAIL** — drop `session_conf` from `date_math_under_tokyo_session`:

```text
$ pytest python/repark/tests/test_parity_live.py -q -k registry
>       assert len(overridden) >= 2, "at least two scenarios must run under a non-UTC oracle session"
E       AssertionError: at least two scenarios must run under a non-UTC oracle session
E       assert 1 >= 2
1 failed, 1 passed, 63 deselected in 0.39s
```

**must-PASS** — override restored:

```text
$ pytest python/repark/tests/test_parity_live.py -q -k registry
2 passed, 63 deselected in 0.35s
```

**Tree check.** All three provocations were reverted by restoring a pre-provocation copy of each
file; `git diff` against the staged tree is empty for
`crates/repark-core/src/session_time_zone.rs`, `python/repark/tests/_live_parity.py` and
`python/repark/tests/test_session_timezone_parity.py`, and the final `make ci` / `make test` /
facade runs above are post-revert.

*Amended in the fix pass:* a `git diff` check is NOT sufficient on its own. A stale pytest
assertion-rewrite cache in `python/repark/tests/__pycache__` survived a provocation revert and
produced a twice-consecutive false RED for one panel lens (finding S-8). Every revert in §8.2 was
verified by **sha256 of the file AND a cold `__pycache__`**, and all `*.pyc` under `python/` and
`scripts/` were deleted before the §8.3 gates were run.

---

## 6. Divergence-registry rows (ready to paste — this unit must NOT create the file)

`docs/spark-sql-iceberg-parity.md` is authored by unit **H-1d**, which merges FIRST. The rows below
are this unit's output for it, in the four-column shape the brief mandates (repark behavior /
Spark behavior / the pin / rationale). The orchestrator lands them at assembly.

**Declared-divergence rows**

| Row | repark behavior | Spark behavior | Pin | Rationale |
|---|---|---|---|---|
| **TZ-1 — timestamp extraction ignores the session zone** | `year` / `month` / `dayofmonth` / `hour` / `date_trunc` / `date_format` over a TIMESTAMP resolve in the STORED (UTC) zone; `spark.sql.session.timeZone` does not move them | resolves every one in the session zone (the census measured a four-hour offset) | `python/repark/tests/test_session_timezone_parity.py::test_session_timezone_row_matches_spark_or_still_diverges` — **15 extraction-class disclosure rows** (the module holds 18 disclosure table rows + 2 equality controls; of the 18, two are TZ-4 and one is TZ-5), e.g. `[hour_of_instant_under_new_york_session]`, `[dst_fall_back_repeated_local_hour]`, `[year_boundary_date_trunc_under_tokyo_session]`, and the two COLUMN-path rows `[column_extract_under_new_york_session]` / `[column_extract_under_tokyo_session]` | **IN-FLIGHT FIX** (campaign decision D7; H-1a split B). Recorded as a disclosure so the CRITICAL class is measured while the fix lands; every row flips to an equality assertion with the fix, which is the fix's revert-red evidence. |
| **TZ-2 — the session-zone default is `UTC`** | `spark.conf.get("spark.sql.session.timeZone")` is `UTC` on a session that never set it | the JVM's local zone, so results depend on the host | `test_session_timezone_conf_is_readable_back_and_defaults_to_utc`; `crates/repark-core/src/session_time_zone/tests.rs::absent_key_resolves_to_the_utc_default` | **DECLARED.** A reproducible default beats a host-dependent one, and reading the host zone would be the query-time/construction-time environment read `docs/adr/0004-server-prep-disciplines.md` forbids. A job that wants host-local behavior sets the key. |
| **TZ-3 — a runtime `conf.set` of the session zone is accepted, neither validated nor applied** | the call succeeds, warns (`accepted for source compatibility but NOT applied ... its value is NOT validated`) and stores nothing, so `conf.get` keeps reporting the engine's real zone; `conf.unset` behaves the same way. **The value is not checked at all**: `conf.set(K, "Mars/Olympus_Mons")` is swallowed, where live Spark raises `[INVALID_CONF_VALUE.TIME_ZONE] ... SQLSTATE: 22022`. **The disclosure is once per PROCESS, not per session** (the OTH-010 `_warn_master_once` idiom), so a second session in the same interpreter gets a fully SILENT no-op. | applies the new zone to the live session immediately, and validates it | `test_runtime_conf_set_of_the_session_zone_is_accepted_but_not_applied` (covers the valid leg, the warning TEXT saying "NOT validated", and the invalid-value leg under `simplefilter("error")` so a second warning would raise); `test_apache_sql_conf_context_manager_round_trips_the_session_zone`; `test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise` (the same laxness on the reuse path) | **DECLARED**, and evidence-driven: refusing broke the pinned Apache drop-in test `test_create_dataframe_from_pandas_with_dst`, which sets this key via PySpark's `sql_conf`. Accepting keeps drop-in; not storing keeps `conf.get` honest. The unvalidated half is the accepted COST of keeping exactly one validator (in the engine, at build) — repark is knowingly laxer than PySpark on this key at runtime, and the warning text now says so in as many words rather than leaving a typo'd zone to a silent no-op. Becomes fixable if split B routes the zone through DataFusion `ConfigOptions`. |
| **TZ-4 — TIMESTAMP Arrow export is tz-naive** | `to_arrow()` yields `timestamp[ns]` (or `timestamp[us]` after `date_trunc`) with NO timezone | `toArrow()` yields `timestamp[us, tz=UTC]` | `[to_timestamp_of_zone_suffixed_string]`, `[tz_aware_to_naive_round_trip]`, `test_current_timestamp_type_and_zone_disclosure` | **IN-FLIGHT FIX** — the export-type half of TZ-1's class. A consumer that localizes a tz-naive column silently shifts it, which is why the type is asserted and not only the value. |
| **TZ-5 — `CAST(TIMESTAMP AS BIGINT)` returns nanoseconds** | epoch NANOSECONDS (`-1800000000000` for 1969-12-31T23:30:00Z) | epoch SECONDS (`-1800`) | `[pre_1970_timestamp_cast_to_bigint]` | **NEW, found by this unit; NOT a zone bug** — a cast-unit bug, correctly signed before 1970. Disclosed here; the fix needs its own unit (a 10^9 factor on every timestamp->integer cast is a silently-wrong-result class in its own right). |

**Backlog rows (no pin yet — surfaced by this unit while authoring the corpus)**

| Row | repark behavior | Spark behavior | Status |
|---|---|---|---|
| **B-TZ-1 — `unix_timestamp` is not a Spark-door SQL function** | `SELECT unix_timestamp(...)` fails: `Error during planning: Invalid function 'unix_timestamp'. Did you mean 'to_timestamp'?` | returns epoch seconds | BACKLOG. The facade exposes a Python `F.unix_timestamp`; only the SQL-door spelling is missing, so this is a two-doors coverage hole, not a missing kernel. |
| **B-TZ-2 — `timestamp_seconds` is not a Spark-door SQL function** | `SELECT timestamp_seconds(-1800)` fails: `Invalid function 'timestamp_seconds'` | builds a TIMESTAMP from epoch seconds | BACKLOG, same shape as B-TZ-1 (a `F.timestamp_seconds` exists on the facade). |
| **B-TZ-3 — `date_add(DATE, <integer literal>)` fails to coerce in the SQL door** | `Failed to coerce arguments ... 'date_add(Date32, Int64)' ... You might need to add explicit type casts` | returns a DATE | BACKLOG. Spark's own idiom (`date_add(d, 1)`) does not plan; the DataFrame-API spelling does. |
| **B-TZ-4 — `CAST(TIMESTAMP AS STRING)` returns `string_view` and ISO-`T` formatting** | Arrow type `string_view`; rendered `2024-06-15T12:00:00` in the stored zone | Arrow type `string`; rendered `2024-06-15 08:00:00` in the session zone | BACKLOG (observed while authoring; the round-trip row was deliberately narrowed to the timestamp column so one row does not conflate the zone class with the string-type class). |
| **B-TZ-5 — the SQL `SET` door does not reach the `spark.*` conf namespace** | `spark.sql("SET spark.sql.session.timeZone = 'Asia/Tokyo'")` (and the backtick-quoted / no-space spellings) raise `datafusion engine error: Invalid or Unsupported Configuration: Could not find config namespace "spark"` | applies the conf | BACKLOG, surfaced by the panel. **Pre-existing for every `spark.*` key, not introduced by this unit**, and wider than the session zone — it wants its own decision rather than a fold into split B. Recorded because D-A5's drop-in argument covers the Python `conf.set` door and must not be read as covering the SQL door too. |

---

## 7. Residuals, for the verifier and for split B

> **Disposition, added 2026-08-10 when split B landed** (the list below is split A's record and is
> not rewritten): **#1** (the four-entry-point matrix) is DISCHARGED — §B.3. **#3** (the plumbing
> seam) is DISCHARGED — §B.1/§B.2, and the seam chosen is neither of the two this residual named:
> the zone travels on the session's `ConfigOptions` in a non-settable carrier, installed from the
> `SparkExtension::configure` hook, because it must be readable at INVOKE time for the DataFrame-API
> entry point to work at all (§B.2 D-B1). **#4** (DIVERGENCE-1) is DISCHARGED — revisited, left
> unrowed, docstring corrected (§B.6). **#8** (the SQL `SET` door / B-TZ-5) was RE-EVALUATED and
> deliberately NOT taken (§B.2 D-B2, rejected alternative). **#2, #5, #6, #7, #9 are untouched** by
> split B and stand as written.

1. **Four-entry-point matrix is split B's, claimed as split-scope in the §3 gate table.** Split A
   pins ONE cell — the facade `sql()` door — and now pins it over a tz-aware timestamp COLUMN as
   well as over scalar literals. The native-DataFrame / ANSI-door / Spark-door cells are UNPINNED
   here. The original reason given ("until extraction honors the zone the other cells would pin
   the same absence again") was refuted at review: pinning an absence per cell is precisely what
   this corpus does, and the panel demonstrated the DataFrame-API cell diverges identically today
   (`df.select(F.year(F.col('ts')), F.hour(F.col('ts')))` under `America/New_York` → y=[2024,2024],
   h=[3,4]). Split B must add the remaining three cells WITH the fix. Recorded as a claim, not a
   silence.
2. **Reuse-path validation asymmetry** (D-A1): an invalid zone passed to a second `getOrCreate`
   is not validated, because no session is built. It is also not applied — the existing engine-knob
   warning fires and `conf.get` still reports the engine's zone. This is now PINNED both ways:
   a valid-but-different zone by
   `test_getorcreate_reuse_with_a_different_zone_warns_and_leaves_the_conf_alone`, an INVALID zone
   by `test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise`. Before the fix pass
   this ledger and the actor report both claimed the invalid leg was pinned when it was not (§8,
   finding S-3). `get_or_create`'s own docstring, which promised validation for every engine knob,
   now carves this key out explicitly.
3. **The plumbing seam for split B is deliberately not chosen here** (D-A2). The zone lives on
   `ReparkSession`; `SparkExtension::configure` already receives the builder conf map and depends on
   both `repark-core` and `repark-functions`, so it is the natural place to hand the zone to the
   extractor layer. Note the tier rule: `repark-core` (tier 2) may NOT depend on `repark-functions`
   (tier-3 leaf), so a core-side import is not an option.
4. **`test_dogfood_gaps.py::test_divergence_timestamp_ltz_collect_passthrough` (DIVERGENCE-1)** says
   in its docstring "disclose, do not build session-tz machinery". Decision D7 supersedes that
   instruction. Split A leaves the test green (it changes no evaluated result); split B must
   revisit that disclosure in the same diff as the fix.
5. **`build_repark_engine(session_conf)` stops the process-active session** when — and only when — an
   override is requested, because `getOrCreate` would otherwise hand back a session carrying the
   previous scenario's zone. A future caller that builds two engines inside one test would find the
   first handle dead. Narrow by construction and documented at the call site.
6. **`docs/port/census.md`'s recorded registry count** (27) was updated to 29 with the reason,
   because that file states the count must "only ever move deliberately". It is the only file
   outside this unit's own surface that this unit edits, and it is a doc-only edit.
7. **Three new engine gaps and one type gap were found while authoring** (registry rows TZ-5,
   B-TZ-1..B-TZ-4). None is fixed here. TZ-5 in particular is a silently-wrong-result class
   (a 10^9 factor) that deserves its own unit rather than a fold into split B.
8. **The SQL door cannot set this conf at all**, and the drop-in argument in D-A5 must not be
   read as broader than it is: `spark.sql("SET spark.sql.session.timeZone = 'Asia/Tokyo'")` raises
   `Invalid or Unsupported Configuration: Could not find config namespace "spark"` — as does every
   `spark.*` key through the SQL door. **Pre-existing, not introduced here**, and NOT fixed here.
   Split B item: it is a candidate divergence-registry row (working name **B-TZ-5 — the SQL `SET`
   door does not reach the `spark.*` conf namespace**), and it is wider than the session zone, so
   it wants its own decision rather than a fold into the extraction fix. The Python `conf.set`
   door is the one D-A5's evidence actually covers.
9. **The `timeZone` reader option is a different surface.** A grep for `timezone`-shaped strings in
   `python/repark/src` also finds `reader.py` / `_funcs.py` occurrences: those are the
   `spark.read.option("timeZone", ...)` READER option that predates this unit, not a session conf
   key. The one-spelling claim is about the session-conf surface, and the grep in §4 shows exactly
   one string literal per language.

---

## 8. Fix pass (panel findings) — 2026-08-10

The FULL adversarial panel (semantics + oracle lenses) returned **ACCEPT-WITH-NITS** from both
lenses, no BLOCKER, with 6 + 3 MAJORs and 8 NITs. Both lenses independently reproduced the
headline claims — resolved-once, one-spelling, the evidence behind the D-A5 reversal, and the
oracle: one lens re-derived 14 of the recorded Spark halves and the other re-derived all 18 plus
the `current_timestamp` type, from drivers of their own, against live PySpark 4.1.2. Every finding
below is recorded with what was actually DONE, including the three places the dispositions were
followed but the underlying claim in this ledger turned out to have been wrong.

Gates after the fix pass are in §8.3; the provocations the fix pass altered or added are in §8.2.

### 8.1 Finding → action

| # | Sev | Finding | Action |
|---|---|---|---|
| S-1 | MAJOR | The runtime `conf.set` path performs ZERO validation, so repark silently swallows a zone live PySpark refuses — and is fully silent after the first call in the process. | **KEPT accept-warn-don't-store** (the D-A5 evidence stands), made HONEST and PINNED instead of tightened. The warning now states in words that the value is *neither validated nor applied*, that validation happens once at `getOrCreate`, and that the disclosure is once per process. Pinned by `test_runtime_conf_set_of_the_session_zone_is_accepted_but_not_applied`, which now asserts the warning TEXT contains "its value is NOT validated" and drives an invalid zone (`Mars/Olympus_Mons`) under `simplefilter("error", UserWarning)` so the silent-second-call behavior is pinned, not merely true. Registry row TZ-3 rewritten to disclose the unvalidated half and the once-per-process scope. |
| S-2 | MAJOR | `get_or_create`'s docstring promises validation for every engine knob on the reuse path; the zone is now in that set and is not validated. | Docstring CORRECTED at `session_core.py`: the promise is now scoped to knobs whose validation is facade-side, with an explicit carve-out naming `spark.sql.session.timeZone`, why (zone validity needs the engine's zone database and the repo keeps ONE validator), and the pin that holds it. |
| S-3 | MAJOR | This ledger (§7.2) and ACTOR-REPORT §8.2 both claimed the invalid-zone-on-reuse behavior was "pinned by `test_getorcreate_reuse_with_a_different_zone_warns_and_leaves_the_conf_alone`". That test passes a VALID zone. The claim was false. | Pin ADDED: `test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise` — with an active Tokyo session, a second builder carrying `Mars/Olympus_Mons` warns the engine-knob warning, returns the ACTIVE session, does not raise, and leaves `conf.get`/`conf.getAll` on `Asia/Tokyo`. §7.2 rewritten to name both pins. Provoked (§8.2 P-5). |
| S-4 | MAJOR | `python/repark/tests/map.md` still advertised "the **loud refusal** of a runtime `conf.set`" — the behavior the same diff reversed. Two maps in one diff disagreed on the unit's headline behavior. | Reworded to "the accepted-but-neither-validated-nor-applied runtime `conf.set`/`unset` disclosure", and the same entry now also names the reuse laxness, the whitespace normalization and the blank-zone refusal. Swept for other survivors of the reversal: none found. |
| S-5 / O-5 | MAJOR / NIT | `conf.get` does not "ALWAYS report the zone the live engine session actually has": the engine trims, the facade did not, so `.config(K, "  Asia/Tokyo \t")` made the two disagree. | `normalize_session_time_zone_config` added to `session_time_zone.py` and called from `get_or_create` **before** the value is stored or forwarded. Its docstring and both call-site comments state that this is WHITESPACE NORMALIZATION ONLY and that the engine (`SessionTimeZone::parse`) remains the SOLE validator. Module docstring's "ALWAYS" claim rewritten to say what the mechanism is rather than assert an inference. Pinned by `test_padded_zone_is_normalized_so_conf_get_reports_the_engine_zone`; the failure mode of over-reaching is pinned by `test_whitespace_only_zone_still_fails_loud_at_session_build` (`"   "` trims to empty and the ENGINE still refuses — normalization must never turn a refusal into a silent `UTC`). Provoked (§8.2 P-6). |
| S-6 | MAJOR | The zone validator rides an UNDECLARED transitive `arrow/chrono-tz` enable from `datafusion-functions`. Without it `Tz::from_str` accepts only fixed offsets and rejects EVERY IANA id, so a DataFusion feature change would turn `.config(K, "America/New_York")` into a hard build refusal for every user. | Declared: `crates/repark-core/Cargo.toml` now carries `arrow = { workspace = true, features = ["chrono-tz"] }` with the reason inline. `cargo tree -p repark-core -e features -i arrow-array` now lists `repark-core` as a DIRECT enabler of `arrow feature "chrono-tz"` beside `datafusion`. **`Cargo.lock` is byte-unchanged** (sha256 verified before and after; `git diff --cached Cargo.lock` empty) — the feature was already resolved, only the ownership moved. Debug rows added to `crates/repark-core/map.md` and `crates/repark-core/src/session_time_zone/map.md` naming the exact symptom ("every IANA id refused, fixed offsets still work"). |
| O-1 / S-NIT | MAJOR / NIT | The flip-don't-delete guidance sat on `assert _frames_differ(row.repark, row.spark)` — two module CONSTANTS, so no engine change can ever reach it. A real convergence red on the first assertion with a bare `FrameMismatchError`. The docstring credited the wrong assertion. | The disclosure runner now CLASSIFIES the failure before raising, on `actual` (the engine's real output): if `actual` equals the recorded Spark golden it raises the CONVERGED / flip-don't-delete message; otherwise it raises a REGRESSION message pointing at record mode. The constants assertion is KEPT and re-labelled as what it is — the row-well-formedness guard against a half-edited row — with its own message. Docstring now credits the mechanism it actually has. Both directions re-provoked (§8.2 P-2b convergence, P-4 regression), plus the must-PASS. |
| O-2 | MAJOR | No row exercises a tz-aware timestamp COLUMN; all 19 rows are scalar-literal expressions, so the brief's own recipe wording is met by nothing. The stated reason for deferring was refuted. | Column family ADDED and RECORDED from live PySpark 4.1.2: `column_extract_under_new_york_session` and `column_extract_under_tokyo_session` run `year`/`month`/`dayofmonth`/`hour` over a two-row in-memory frame (`createDataFrame` + `createOrReplaceTempView`, schema INFERRED so both engines carry `timestamp[us, tz=UTC]` — a DDL `ts timestamp` string makes repark's column tz-NAIVE and would have weakened the row). New York moves all four fields including the YEAR across a boundary (`[2023,2024]/[12,6]/[31,15]/[23,8]`); Tokyo moves only the hour (`[13,21]`) while repark answers `[2024,2024]/[1,6]/[1,15]/[4,12]` under BOTH zones — which is what separates a session-zone bug from an offset-sign bug. Budget pins moved in the same diff (G1 12 → 14, still inside 10–14), and `test_session_timezone_row_set_covers_both_gap_budgets` now asserts the column family spans both zones. The four-entry-point matrix is stated as split B's obligation in the §3 gate table and residual §7.1 — claimed, never silent. |
| O-3 | MAJOR | `build_repark_engine`'s `active.stop()` branch executes ZERO times under the suite (conftest clears the active session before every test), while its absence silently leaves a scenario on the wrong zone. | Pin ADDED (JVM-free): `test_build_repark_engine_override_stops_the_active_session_and_rebuilds` builds a default engine, asserts it is the process-active session, then requests a Tokyo override and asserts a NEW session was built, that it carries `Asia/Tokyo`, that it is now the active one, and that the previous handle is STOPPED (the documented cost in residual §7.5). Provoked (§8.2 P-7). |
| O-4 | NIT | The record driver was scratch outside the worktree, so "recorded against live PySpark 4.1.2" became unfalsifiable from inside the repo the moment that session ended. | Driver COMMITTED as `python/repark/tests/_record_session_timezone_goldens.py` (not a `test_` module; never collected). It imports `ROWS` from the committed corpus and runs each row through the SAME `run_row` helper the assertions use, so recipe and oracle are one piece of code. Exit 0 = all halves reproduce; non-zero prints the live schema+rows. It never edits the corpus. Invocation is in the corpus module's docstring, in `python/repark/tests/map.md` (Contents, an `I want to…` row and a Debug row) and in §4.6. Re-run in the fix pass: **20 rows re-derived, 0 mismatch(es)**. |
| S-7 / O-6 | NIT | The "verbatim" facade count (2593) does not reproduce; both lenses got 2594 on the identical tree and command. | Corrected in §4.3 with the honest reason: collection is deterministic from the file tree, so this was not environment sensitivity — the number was captured one test before the final tree. Fix-pass re-capture recorded: **2600 passed / 36 skipped** (record provisioning) and **2564 / 46** for `make py-test-facade` verbatim, the +6 being this pass's new tests. |
| S-8 | NIT | Stale pytest assertion-rewrite cache in the worktree produced a twice-consecutive false RED on a disclosure row. | All `*.pyc` under `python/` and `scripts/` deleted at the start of this pass (gitignored; no tracked file touched). Every gate in §8.3 was run with a cold cache. |
| S-9 | NIT | The disclosure is once per PROCESS, so a second session in the same interpreter gets a fully silent no-op. | Kept (the OTH-010 idiom is consistent) and DISCLOSED rather than fixed: stated in the warning text itself, in the function docstring, in `python/repark/src/repark/session/map.md`'s Debug table as its own row, and in registry row TZ-3. |
| S-10 | NIT | The drop-in argument does not hold for the SQL door: `SET spark.sql.session.timeZone=…` explodes (`Could not find config namespace "spark"`), pre-existing for every `spark.*` key. | Recorded as residual §7.8 and as a **split-B candidate registry row B-TZ-5**. Not fixed here: it is wider than the session zone and wants its own decision. The D-A5 evidence covers the Python `conf.set` door, and the ledger now says so. |
| O-7 | NIT | The ready-to-paste TZ-1 row miscounts its own pins ("15 disclosure rows"); the lens computed 13. | Corrected AND re-derived, because the count MOVED in this pass: the corpus now holds 20 rows = 18 disclosure table rows + 2 equality controls; of the 18, two are TZ-4 and one is TZ-5, leaving **15 extraction-class rows**. The number 15 was wrong when it was written (the true value was 13) and is right now only because the two column rows were added — recorded here so nobody reads the unchanged digit as "no finding". Counted mechanically, not by hand. |

### 8.2 Provocations added or re-run in this pass

Every provocation was reverted from a pre-provocation copy and the restore verified by sha256 +
`git diff`. The must-PASS leg is captured beside each must-FAIL.

**P-2b — the disclosure runner detects a REAL engine convergence** (this is the leg O-1 says the
original could not reach). Simulated honestly: the goldens are UNTOUCHED and the RECIPE is changed
so the engine produces exactly the recorded Spark value
(`year(to_timestamp('2024-01-01T04:30:00Z'))` → `year(to_timestamp('2023-06-01T00:00:00Z'))`,
which answers 2023 in UTC = the recorded Spark half).

```text
$ pytest "...::test_session_timezone_row_matches_spark_or_still_diverges[year_of_instant_under_new_york_session]" -q
        try:
            assert_frames_equal(actual, row.repark)
        except FrameMismatchError as mismatch:
            if not _frames_differ(actual, row.spark):
>               raise AssertionError(
E               AssertionError: year_of_instant_under_new_york_session: repark and Spark have
E               CONVERGED — repark now produces the RECORDED SPARK output, so this disclosure is
E               stale. Do not delete the row: flip it to an equality row (repark=None) and record
E               the convergence. the instant is 2023-12-31 23:30 in New York, so Spark's year is
E               2023; repark extracts in the stored (UTC) zone and answers 2024. Flipped to
E               equality by the session-timezone extraction fix (briefs/v2-engine-hardening.md,
E               H-1a split B).
1 failed in 0.45s
```

**P-4 — a plain REGRESSION is reported as a regression, not as a convergence** (the panel's P-A,
re-run against the new runner): move the row's `repark` half to 2023 so the engine matches neither
half.

```text
$ pytest "...[year_of_instant_under_new_york_session]" -q
>           raise AssertionError(
E           AssertionError: year_of_instant_under_new_york_session: repark moved OFF its pinned
E           disclosure and does NOT match the recorded Spark golden either — this is a regression,
E           not a convergence. Re-derive both halves in record mode (see this module's docstring)
E           before touching the pin. ...
1 failed in 0.44s
```

**must-PASS** (file restored; sha256
`55dd6bdacee61c0400b997f24569e680f626dc708fd8668786d112026ee81939`):

```text
$ pytest "...[year_of_instant_under_new_york_session]" -q
1 passed in 0.37s
```

**P-5 — the reuse-path pins red when the zone leaves the engine-knob set.** Drop
`*SESSION_TIME_ZONE_KEYS` from `engine_key_set` in `session_core.py`:

```text
$ pytest python/repark/tests/test_session_timezone_parity.py -q -k reuse
>       with pytest.warns(UserWarning, match="engine knobs are fixed at session build"):
E       Failed: DID NOT WARN. No warnings of type (<class 'UserWarning'>,) were emitted.
FAILED ...::test_getorcreate_reuse_with_a_different_zone_warns_and_leaves_the_conf_alone
FAILED ...::test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise
2 failed, 28 deselected in 0.40s

$ (restored)  pytest ... -q -k reuse
2 passed, 28 deselected in 0.34s
```

**P-6 — the trim normalization is load-bearing.** Remove the
`normalize_session_time_zone_config(self._config)` call from `get_or_create`:

```text
$ pytest python/repark/tests/test_session_timezone_parity.py -q -k "padded or whitespace_only"
>       assert session.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO
E       AssertionError: assert '  Asia/Tokyo \t' == 'Asia/Tokyo'
FAILED ...::test_padded_zone_is_normalized_so_conf_get_reports_the_engine_zone
1 failed, 1 passed, 28 deselected in 0.40s

$ (restored)  pytest ... -q -k "padded or whitespace_only or invalid_zone"
3 passed, 27 deselected in 0.34s
```

Note the second leg stayed GREEN under the provocation: `test_whitespace_only_zone_still_fails_loud_at_session_build`
pins that normalization does not swallow a blank zone, and removing the normalization does not
break that — which is the correct shape (the two tests pin opposite failure modes).

**P-7 — the `active.stop()` branch is load-bearing.** Replace the `active.stop()` inside
`build_repark_engine` with `pass`:

```text
$ pytest "python/repark/tests/test_parity_live.py::test_build_repark_engine_override_stops_the_active_session_and_rebuilds" -q
>       assert second.session is not first.session, "an override must BUILD, never reuse"
E       AssertionError: an override must BUILD, never reuse
E       assert <ReparkSession object at 0x7a8aea883900> is not <ReparkSession object at 0x7a8aea883900>
  UserWarning: Using an existing ReparkSession; some configuration may not apply (engine knobs are
  fixed at session build; unapplied keys: ['spark.sql.session.timeZone']).
1 failed, 1 warning in 0.46s

$ (restored; sha256 280f83551ebf558e61f0c42d93d495d08aee665e0939a2aa5c358636f69a9513)
1 passed in 0.37s
```

That warning line in the RED output is the finding stated as evidence: without the stop, the
override is swallowed by the reuse path and the scenario runs under the previous zone.

P-1 (one spelling, Rust) and P-3 (the non-UTC registry pin) were not altered by this pass and are
unchanged in §5.

### 8.3 Gates after the fix pass

```text
$ make ci
crate-dag: 20 internal edges clean (4 dev, 15 normal, 1 optional) across 9 of 9 mapped crates
lib-rs: 9 crate roots clean (no inline test modules; ceilings held)
lib-py: 54 files clean (ceilings held; no-stub rule held)
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
          index, the status document and the crate maps
uvx ruff@0.15.22 check .     -> All checks passed!
uvx ruff@0.15.22 format --check .  -> 241 files already formatted
uv lock --locked             -> Resolved 29 packages in 1ms
taplo format --check / lint / typos -> clean
EXIT=0

NOTE (honest): the first `make ci` of this pass was RED at `py-lint` on three ruff findings in the
new code (I001 import order in the record driver, RUF043 an unescaped `match=` pattern, E501 a
101-column line). Fixed, not suppressed; the run above is the second.

$ make test                  # cargo test --locked --workspace, never --all-features
31 test binaries, every one `test result: ok`; 0 `test result: FAILED`; 1279 tests passed
EXIT=0

$ PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark/tests -q
2600 passed, 36 skipped, 46 warnings in 142.79s        # was 2594 pre-fix (+2 column rows,
EXIT=0                                                 # +4 conf-surface pins)

$ make py-test-facade
2564 passed, 46 skipped, 37 warnings in 91.47s
EXIT=0

$ JAVA_HOME=zulu-17 SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 pytest python/repark/tests/test_parity_live.py -q
66 passed in 17.54s
$ (gate unset)
33 passed, 33 skipped in 0.79s

$ .venv/bin/python python/repark/tests/_record_session_timezone_goldens.py   # JAVA_HOME set
record mode: 20 rows re-derived, 0 mismatch(es)
EXIT=0

$ bash scripts/check_map_md.sh      -> EXIT=0   (staged-set method; 8 touched dirs' map.md staged)
$ python3 scripts/check_manifest.py -> EXIT=0

$ sha256sum -c (Cargo.lock, taken before the arrow feature declaration) -> Cargo.lock: OK
```

### 8.4 Deviations from the dispositions, stated rather than absorbed

1. **Disposition 1's warning wording kept the substring the existing pins and maps quote.** The
   message now says "…accepted for source compatibility but NOT applied at runtime, and its value
   is NOT validated…", which extends rather than replaces the phrase `pytest.warns(match=…)`,
   `python/repark/src/repark/session/map.md` and registry row TZ-3 all key on. Replacing it would
   have silently changed three documents' quoted text.
2. **Disposition 6's column rows are the facade `sql()` door over a COLUMN, not the DataFrame-API
   door.** The disposition asks for "year/month/day/hour over a tz-aware timestamp COLUMN (a small
   in-memory dataframe)" and separately says the four-entry-point matrix stays split B's. Those two
   are satisfied by one recipe on the existing door; adding a DataFrame-API row would have started
   the matrix this split is explicitly not to start. The frame IS built with `createDataFrame` and
   the column IS `timestamp[us, tz=UTC]` on both engines, so the brief's recipe wording is met.
3. **Disposition 9's TZ-1 count did not need the correction the panel computed.** The lens's
   arithmetic (13) was right for the tree it read; the two column rows this pass adds move it to
   15, which is the number the cell already carried. It is recorded as a corrected count with the
   working shown, so the coincidence is visible instead of looking like the finding was ignored.
4. **Nothing was tightened toward Spark.** Dispositions 1 and the S-1 finding invite a stricter
   runtime `conf.set`; the orchestrator's disposition is accept-warn-don't-store, and the D-A5
   evidence (a pinned Apache drop-in test) still holds. The laxness is now disclosed in the
   warning text, in two maps, in a pin and in the registry row — not removed.

---

## § Split B — the extraction fix (2026-08-10)

**Unit:** H-1a **split B** of the V2 Engine Hardening campaign
([../briefs/v2-engine-hardening.md](../briefs/v2-engine-hardening.md) "H-1a", split rule: *"unit B
= the extraction fix + its Rust pins"*). Split A changed no evaluated result; **this half changes
evaluated results on purpose**, and every row it moves is a row split A recorded first.

### B.1 What landed

| Layer | File | What changed |
|---|---|---|
| Engine seam | `crates/repark-core/src/extension.rs` | `SessionExtension::configure` now takes a `SessionBuildConf` — the builder conf map PLUS the values `build()` already resolved from it (today, the session timezone) |
| Engine | `crates/repark-core/src/session.rs` | `build()` hands the ONE resolved `SessionTimeZone` to the hook; it is still resolved exactly once |
| Carrier | `crates/repark-functions/src/session_time_zone.rs` (NEW, + `session_time_zone/`) | a `ConfigExtension` that carries the resolved zone on the session's `ConfigOptions`, with a `set` that always refuses and empty `entries()` — a channel, not a knob |
| Semantics | `crates/repark-functions/src/datetime.rs` | the coercion path declares which arguments are INSTANTS; `DatePartUdf`, `DateTrunc` and `DateFormat` resolve those instants in the session zone at invoke |
| Door | `crates/repark-spark/src/extension.rs` | `configure` installs the carrier — the one crossing point between the crate that owns the key and the crate that owns the extractors |
| Feature | `crates/repark-functions/Cargo.toml` | `arrow`'s `chrono-tz` DECLARED (same reasoning as split A finding S-6; `Cargo.lock` byte-unchanged) |

**The mechanism, in one paragraph.** Spark's `TIMESTAMP` is an instant and every calendar field it
exposes over one is read in `spark.sql.session.timeZone`. `coerce_date_arg` /
`coerce_to_timestamp_micros` now say which arguments are instants by *typing* them: a `Timestamp`
of any unit and any zone — including **none** — coerces to a `UTC`-annotated timestamp, while
`Date32` / `Date64` / `Time32` / `Time64` / string arguments keep zone-free types. At invoke the
extractors read the zone out of `ScalarFunctionArgs::config_options` and resolve only the
tz-annotated arguments in it: `DatePartUdf` re-annotates the array (metadata only — arrow's ticks
are epoch-relative either way, so the instant is never moved) and lets `date_part` read the local
calendar; `DateTrunc` and `DateFormat` go through one shared `invoke_local_micros`, and `date_trunc`
puts its locally-truncated result back on the timeline with `java.time`'s DST rules (earliest
offset when the local time is ambiguous, pushed forward across a spring-forward gap).

### B.2 Decisions, with rationale

**D-B1 — The zone is read at INVOKE time, not baked into the UDF at registration. This is the
decision the whole seam turns on, and it was forced by evidence, not taste.**
`repark_functions::expr_fn` builds a **standalone** `Expr` that embeds a UDF *instance* — that is
its documented reason to exist, because `repark-python`'s `PyColumn` has no `SessionContext` to
resolve a name against. So the obvious design (construct zone-aware UDFs in `register_all`) reaches
both SQL doors and **misses the DataFrame API entirely** — the exact cell split A's residual §7.1
says split B owes. `ScalarFunctionArgs::config_options` (DataFusion 54.1) is populated for the
executing session on both paths, including the constant-folding path
(`ConstEvaluator::try_new(config_options)` — measured, and 18 of the 20 facade rows are scalar
literals that fold). `native_dataframe_api_extracts_in_the_session_zone` is the pin that fails if
this is ever "simplified" back.

**D-B2 — `datafusion.execution.time_zone` was EVALUATED and REJECTED; the carrier is a
repark-owned `ConfigExtension` whose `set` refuses.** Split A's D-A2 handed split B this choice and
the brief asked for it to be decided on evidence. Measured, in DataFusion 54.1:

1. *What the option actually drives.* `now()` / `current_timestamp` (its return **type**'s zone),
   `current_date`, `current_time`, the SQL planner's `TIMESTAMP WITH TIME ZONE` → Arrow type
   mapping, and `datafusion-spark`'s own `date_trunc` / `cast`. It does **not** drive `date_part`:
   arrow's calendar kernels read the *array's* zone annotation. Measured directly — a probe run at
   `datafusion.execution.time_zone = 'Asia/Tokyo'` returned `now()` as
   `Timestamp(Nanosecond, Some("Asia/Tokyo"))` while `date_part(hour)` over a `timestamp[us, UTC]`
   array still answered `12` for `2024-06-15T12:00Z`. **So setting it would not have fixed the
   class**; the extractor change was needed either way.
2. *It is a second live spelling.* `.config("datafusion.execution.time_zone", …)` reaches
   `SessionConfig` through `apply_datafusion_config_keys`, and `SET datafusion.execution.time_zone`
   reaches it through the SQL door. The unit's acceptance gate is "a session-conf grep shows
   exactly ONE authoritative spelling", and two working spellings is exactly what D-A4 refused.
3. *It moves `current_timestamp` the wrong way.* Its type would become
   `timestamp[ns, tz=<session zone>]`; live Spark's is `timestamp[us, tz=UTC]` **whatever** the
   session zone is (the recorded oracle shows `timestamp[us, tz=UTC]` under an
   `America/New_York` session). That is further from parity, not closer.

*Rejected alternative, recorded:* a `ConfigExtension` with `PREFIX = "spark"`, which WOULD make
`SET spark.sql.session.timeZone = '…'` work and would retire registry queue item B-TZ-5 for this
key. Declined because registering the `spark` namespace changes the error for **every** `spark.*`
key through the SQL door — B-TZ-5 is explicitly wider than the session zone and "wants its own
decision" (split A residual §7.8), and quietly making that decision inside an extraction fix is the
scope creep this ledger exists to prevent.

*What the chosen carrier costs and why it is still not a spelling:* DataFusion resolves an
extension namespace on the text before the FIRST `.`, so a two-segment `PREFIX` (`repark.session`)
is unreachable from `SET` — the same accident that already makes the neighbouring `repark.sql`
extension unreachable, here made deliberate and pinned
(`the_sql_set_door_cannot_reach_the_carrier`). On top of that the carrier's `set` **always refuses,
naming `spark.sql.session.timeZone`**, and its `entries()` is empty so it never appears as a
settable option. The grep still shows one authoritative spelling per language.

**D-B3 — TZ-3 does NOT retire.** It follows from D-B2: a runtime `spark.conf.set` of the zone
remains accepted-warned-not-stored, because the carrier is deliberately not settable and
`datafusion.execution.time_zone` was rejected. The registry row now carries the measurement rather
than the old speculative "becomes fixable if…" sentence. Making `conf.set` real is still possible —
it needs a session-level re-registration path, not a config key — and it is a decision for the unit
that owns TZ-4, because that unit is already changing the timestamp representation.

**D-B4 — TZ-4 SPLITS AGAIN, and here is exactly why (the brief demands this be said in words).**
The fix closes the **value** half of the class and not the **type** half. `date_trunc`'s output
stays `timestamp[us]` tz-naive; `to_timestamp('…Z')` still yields `timestamp[ns]` tz-naive. The
reason is mechanical: TZ-1 lived in the extractor *coercion path* and was fixed there, while TZ-4
is repark's TIMESTAMP **representation** — the unit (`ns` vs Spark's `us`) and the missing `UTC`
annotation are produced by `to_timestamp`, by literal planning, by `CAST` and by the Arrow export,
**none of which is an extractor**. Closing it means changing the engine's timestamp type
everywhere at once; the blast radius includes every facade test that asserts a timestamp type and
the Iceberg write path's `timestamp` vs `timestamptz` column choice. Two rows make the seam
visible: `[date_trunc_day_across_a_zone_boundary]` and
`[year_boundary_date_trunc_under_tokyo_session]` **converged on value and did not converge on
type**, so their `repark` half was re-recorded in this change and they moved from TZ-1's pin list
into TZ-4's. They are the honest cost of the split, not a rounding error in it.

**D-B5 — a tz-NAIVE timestamp is read as a UTC INSTANT, and the consequence is disclosed as a new
registry row (TZ-6).** The type-pure alternative — move only tz-*annotated* timestamps — was
considered and rejected on measurement: `to_timestamp('2024-01-01T04:30:00Z')` yields a tz-naive
Arrow type holding UTC ticks (that *is* TZ-4), so a type-pure rule would have read repark's own
instants as local wall clocks and closed almost none of the recorded corpus. Reading every
TIMESTAMP as an instant matches Spark's **default** type and closes the CRITICAL class. The cost is
real and is now a row: repark's facade spells `TimestampNTZType` but maps it to the same Arrow type
as `TimestampType`, so a column a user means as NTZ moves with the session zone where Spark's would
not. Registry row **TZ-6**, pinned by `a_tz_naive_timestamp_is_read_as_a_utc_instant`. It retires
when TZ-4 does — at which point the rule becomes type-driven for free.

**D-B6 — the `configure` hook signature changed rather than the door re-resolving the key.**
`SparkExtension::configure` already receives the conf map, so it *could* have called
`repark_core::resolve_session_time_zone` itself. That would have been a second resolution of a value
the engine had already settled, and split A's headline property is "resolved ONCE, at construction".
`SessionBuildConf` costs three implementors (`SparkExtension`, `TaExtension`'s defaulted
pass-through, the core's `RecordingExtension`) and makes the dependency visible at the seam. The
order pin now sets a **padded** zone (`"  Asia/Tokyo "`) so a door that re-parsed the map instead of
taking the resolved value would be caught, not merely absent.

**RISK — this seam is FROZEN, and the first pass changed it without saying so.**
`docs/design/session-api.md` § "Seam freeze (2026-08-08, phase-2 PR-6)" declares `SessionExtension`
frozen and states its own amendment rule: *"changing or removing a method or an existing field now
requires a superseding design note."* `docs/design/sql-doors.md` §3 repeats the freeze. This
decision is exactly that case. The panel caught that no note existed and that the freeze was never
named here; both are now fixed — the note is
[`docs/design/session-extension-conf-seam.md`](../docs/design/session-extension-conf-seam.md)
(dated 2026-08-10), it is linked from both freeze sites, `session-api.md`'s stale signature line is
corrected, and it records the rejected alternative the panel proposed (a defaulted second hook
`configure_with`, refused because two configure positions in a one-position seam is a trap: an
extension implementing only `configure` would silently never see resolved session values).
`SqlDialect::execute`, `register` and the session-scoped-not-dialect-scoped rule are untouched.

### B.3 The four-entry-point matrix — discharged

The brief narrows `docs/testing.md` matrix row 3 into four cells. Split A pinned one and said so;
this is the other three, against the same instants and the same expectations.

| Cell | Where | What it asserts |
|---|---|---|
| **native DataFrame API** | `crates/repark-spark/tests/session_timezone.rs::native_dataframe_api_extracts_in_the_session_zone` | `expr_fn::year/hour` over a registered tz-aware column, through the DataFusion `DataFrame` API — a standalone `Expr` with no session attached, the shape `F.year(col)` takes. It also asserts the Spark door on the SAME session returns the same rows, so the two cells are pinned to each other rather than to two hand-written expectations. |
| **Spark door** | the eight family pins in the same file (`session.sql` on a `SparkDialect` session) | value AND Arrow type, two non-UTC zones, DST included |
| **ANSI door** | `crates/repark-sql/tests/session_timezone_ansi_door.rs::ansi_door_and_spark_door_agree_under_a_non_utc_session` | ONE Spark-extended session at a non-UTC zone: `sql_with(AnsiDialect)` and the Spark door agree, value AND type. It lives in `repark-sql` because the crate-DAG policy allows `repark-sql → repark-spark` as a **dev** edge and nothing the other way, so this is the only binary where the two can meet. |
| **facade** | `python/repark/tests/test_session_timezone_parity.py` | the recorded corpus, thirteen of whose disclosures are now equality rows |

A **single-session** row is the right shape for the ANSI cell and the file says why: extensions are
session-scoped, not dialect-scoped, so a two-session comparison would measure the extension (two
different function registries) rather than the door. The honest negative is pinned beside it —
an extension-free session is stock DataFusion and reads the stored zone, which is a property of
that profile and not a Spark divergence.

### B.4 The Rust extractor pins (budget: 6–8; delivered 8, plus 6 boundary negatives and 1 interpretation pin)

All in `crates/repark-spark/tests/session_timezone.rs`, on real sessions, value AND Arrow type,
under `America/New_York` and `Asia/Tokyo` (plus `Asia/Kolkata` where a half-hour offset is the
point). The LOCATION RULE was followed: this is a NEW file-backed integration surface;
`crates/repark-spark/src/tests.rs` was not touched at all (a parallel unit is splitting it).

| # | Family | Pin | What only this row catches |
|---|---|---|---|
| 1 | `year` | `year_extractor_resolves_in_the_session_zone` | the calendar YEAR crossing at `2024-01-01T04:30Z` |
| 2 | `month` / `dayofmonth` / `dayofyear` | `month_and_day_extractors_resolve_in_the_session_zone` | the leap day moving back one, and Tokyo NOT moving — the pair separates a session-zone fix from an offset-sign one |
| 3 | `hour` / `minute` / `second` | `hour_minute_second_extractors_resolve_in_the_session_zone` | `Asia/Kolkata` (+05:30) moves the MINUTE: a fix that only ever shifts whole hours reds here and nowhere else |
| 4 | `dayofweek` / `weekday` / `weekofyear` / `yearofweek` / `quarter` | `week_and_quarter_extractors_resolve_in_the_session_zone` | the ISO pair moving a whole YEAR at a New Year boundary |
| 5 | `date_trunc` | `date_trunc_truncates_on_the_session_zone_calendar` | truncation on the LOCAL calendar put back on the timeline — and the tz-naive output type, asserted so TZ-4 stays visible |
| 6 | `date_format` | `date_format_renders_in_the_session_zone` | rendering and extraction moving TOGETHER (the self-consistently-wrong partition path) |
| 7 | DST | `dst_boundaries_resolve_like_spark` | spring-forward → 3 (a fixed-offset implementation answers 2) and fall-back collapsing two instants onto local hour 1; plus that the INSTANTS themselves did not move |
| 8 | negative epoch | `pre_1970_instants_resolve_in_the_session_zone` | sign handling surviving the zone change, and Tokyo crossing INTO 1970 |

The other half of the claim is **6 boundary negatives + 1 interpretation pin**, and the
distinction matters because it is the ledger's own test: a *negative* stays GREEN under provocation
P-B1 (it measures the fix's boundary), an *interpretation pin* reds (it is an extraction claim).
Boundary negatives: `date_arguments_never_move_with_the_session_zone`,
`time_arguments_never_move_with_the_session_zone`,
`default_session_extracts_in_the_core_default_zone` (the cross-crate default agreement),
`the_carrier_refusal_names_the_engines_own_key` (the one-spelling mirror, checked across the crate
boundary), and — in `repark-functions` itself —
`coercion_is_idempotent_so_a_second_analysis_cannot_promote_a_date` and
`only_timestamp_arguments_are_coerced_to_instants`. The interpretation pin is
`a_tz_naive_timestamp_is_read_as_a_utc_instant` (D-B5 stated as a decision on the record), which
reds under P-B1 — it was miscounted as a negative in the first pass, and the panel caught it.
*(The 2026-08-10 rework adds six more Rust rows; see §B.10.)*

**The DATE negative earned its keep during the fix, not after it.** The first working draft mapped
`Date32 → Timestamp(µs, None)` in `coerce_to_timestamp_micros`; DataFusion coerces at analysis and
**re-analyzes at physical planning**, so the second pass read that output as "a timestamp" and
promoted it to an instant. `date_format(DATE '2024-02-29', 'yyyy-MM-dd')` rendered `2024-02-28`
under `America/New_York` — a silent one-day shift on the exact path the corpus's control rows
protect. The pin caught it on its first run. The fix was to make every coercion arm a fixed point
(`Date32 → Date32`, `Utf8 → Utf8`, `Timestamp(µs, UTC) → Timestamp(µs, UTC)`) and widen inside the
invoke instead, and the property is now pinned directly rather than left as a worked example.

### B.5 The flips (the revert-red evidence split A promised)

Split A recorded 18 disclosure rows + 2 equality controls. After this fix:

- **13 rows flipped to equality** (`repark=None`): the six `year`/`month`/`day`/`hour` rows, both
  DST rows, both `column_extract_*` rows, the pre-1970 extraction row, the year-boundary
  extract-and-format row and the leap-day row. Reverting the fix reds every one of them — captured
  verbatim in §B.7 — which is exactly the evidence `docs/testing.md` rule 3 asks for.
- **2 rows had their `repark` half RE-RECORDED and stayed disclosures**:
  `[date_trunc_day_across_a_zone_boundary]` and `[year_boundary_date_trunc_under_tokyo_session]`.
  Their value converged, their Arrow type did not, so they moved from TZ-1 to TZ-4 (D-B4). This is
  the one place a recorded half moved, it is a **repark** half and not a Spark half, and it is
  named here because a silently-edited golden is how a corpus stops being an oracle.
- **3 rows were untouched**: `[to_timestamp_of_zone_suffixed_string]` and
  `[tz_aware_to_naive_round_trip]` (TZ-4 — no extractor is involved in either) and
  `[pre_1970_timestamp_cast_to_bigint]` (TZ-5, a cast-unit bug with its own unit).
- **The 2 control rows and the 2 live scenarios are UNCHANGED and still green** — the guard against
  over-reach into the DATE path, and the reason the corpus can tell a zone-aware engine from a
  zone-drunk one.
- **No Spark half was re-recorded.** The record driver reproduces all 20 against live PySpark 4.1.2
  with 0 mismatches both before and after this change (§B.7).

`test_the_extraction_class_converged_and_the_residue_is_named` was added so the SHAPE is pinned too:
it names every remaining disclosure and the class that owns it, so a future red row cannot be
"fixed" by quietly pinning repark's new wrong answer, and a new disclosure cannot be smuggled back
into the extraction class.

### B.6 Registry, STATUS and the DIVERGENCE-1 revisit — in this diff

- **TZ-1 RETIRED** per registry §6 ("a BACKLOG row is retired when the fix lands… the row's pin
  goes RED on purpose in the same change"). Its fifteen pins reddened deliberately; thirteen became
  equality rows and two moved to TZ-4. A short dated note stands where the row was, because §6
  forbids a second authoritative description and a fixed difference is not a divergence.
- **TZ-4 UPDATED** — pin list grows by the two `date_trunc` rows and the Rust type assertion; the
  rationale now states, dated, exactly why it split again (D-B4).
- **TZ-3 UPDATED** — the speculative "becomes fixable if…" sentence replaced by the measurement
  (D-B2). The row itself stands.
- **TZ-6 ADDED** — the NTZ consequence of D-B5, with its pin. It is admitted because the fix is
  what made it observable; it is the only row this unit adds.
- **STATUS.md** — the timezone bullet now records the class as FIXED and points at the two narrower
  rows that remain. The fixing unit removed what it fixed.
- **DIVERGENCE-1 revisited** (split A residual §7.4). `test_divergence_timestamp_ltz_collect_passthrough`
  said "disclose, do not build session-tz machinery" — an instruction decision D7 superseded. The
  pin is deliberately **unchanged**, because a `CAST` is not an extractor and this fix did not touch
  the cast path; its docstring now says so, names TZ-4 and TZ-6 as the classes that keep it
  divergent, and explains that its "session-tz LTZ may have been introduced" alarm still guards the
  cast path specifically. The registry's §1 blind-spot note was updated in the same change, so the
  carve-out no longer reads as open.
- **The live scenario registry was NOT grown.** The brief says size pins move only if the fix forces
  it, and it did not: the two non-UTC scenarios pin DATE invariants and are unchanged. Converting
  the now-converged TIMESTAMP rows into live scenarios is a genuine follow-on (§B.9) and belongs to
  a unit that owns the registry's size pin.

### B.7 Provocation proofs (verbatim)

Neither provocation is committed. Each was reverted from a pre-provocation copy and the restore
verified by **sha256** (split A finding S-8: a `git diff` check alone is not enough, because a stale
pytest assertion-rewrite cache survives a revert); all `*.pyc` under `python/` were deleted before
the must-PASS legs.

#### P-B1 — reverting the extraction fix reds at least one pin per class

*The provocation is the whole fix, at its narrowest point:* make the carrier reader return the
default unconditionally, so the resolved zone never reaches an extractor
(`session_time_zone.rs::session_time_zone_from_options`).

**must-FAIL — the Rust cells (Spark door + native `DataFrame` API):**

```text
$ cargo test --locked -p repark-spark --test session_timezone
test year_extractor_resolves_in_the_session_zone ... FAILED
  left: [2024]                 right: [2023]
test month_and_day_extractors_resolve_in_the_session_zone ... FAILED
  left: [2, 3]                 right: [2, 2]        (both instants are February in EST)
test hour_minute_second_extractors_resolve_in_the_session_zone ... FAILED
test week_and_quarter_extractors_resolve_in_the_session_zone ... FAILED
  left: ([2], [0], [1], [2024], [1])   right: ([1], [6], [52], [2023], [4])
test date_trunc_truncates_on_the_session_zone_calendar ... FAILED
  left: [1718409600000000]     right: [1718337600000000]
test date_format_renders_in_the_session_zone ... FAILED
  left: ["2024-01-01 02:00"]   right: ["2023-12-31 21:00"]
test dst_boundaries_resolve_like_spark ... FAILED
  left: [7, 5, 6]              right: [3, 1, 1]
test pre_1970_instants_resolve_in_the_session_zone ... FAILED
  left: ([1969], [12], [31], [23])    right: ([1969], [12], [31], [18])
test native_dataframe_api_extracts_in_the_session_zone ... FAILED
  left: (2024, 4)              right: (2023, 23)
test a_tz_naive_timestamp_is_read_as_a_utc_instant ... FAILED
  left: [12]                   right: [8]

test result: FAILED. 4 passed; 10 failed; 0 ignored; 0 measured; 0 filtered out
```

The 4 that stayed GREEN are the correct 4: `date_arguments_never_move_with_the_session_zone`,
`time_arguments_never_move_with_the_session_zone`,
`default_session_extracts_in_the_core_default_zone` and
`the_carrier_refusal_names_the_engines_own_key` — none of them is an extraction claim, and a
provocation that reddened them would mean the negatives were measuring the fix rather than its
boundary.

**must-FAIL — the ANSI-door cell:**

```text
$ cargo test --locked -p repark-sql --test session_timezone_ansi_door
test ansi_door_and_spark_door_agree_under_a_non_utc_session ... FAILED
assertion `left == right` failed: 2024-01-01T04:30Z is 2023-12-31 23:00-ish EST; 2024-06-15T12:00Z is 08:00 EDT
  left: [[2024, 2024], [4, 12]]
 right: [[2023, 2024], [23, 8]]
test result: FAILED. 1 passed; 1 failed; ...
    (a_native_session_without_the_spark_extension_reads_the_stored_zone stays green — it pins the
     extension-free profile, which the provocation does not change)
```

**must-FAIL — the facade cell** (native module rebuilt against the provoked engine):

```text
$ pytest python/repark/tests/test_session_timezone_parity.py -q
FAILED ...[year_of_instant_under_new_york_session]
FAILED ...[month_of_instant_under_new_york_session]
FAILED ...[day_of_instant_under_new_york_session]
FAILED ...[hour_of_instant_under_new_york_session]
FAILED ...[hour_of_instant_under_tokyo_session]
FAILED ...[year_month_day_of_instant_under_tokyo_session]
FAILED ...[dst_spring_forward_instant_hour]
FAILED ...[dst_fall_back_repeated_local_hour]
FAILED ...[date_trunc_day_across_a_zone_boundary]
FAILED ...[column_extract_under_new_york_session]
FAILED ...[column_extract_under_tokyo_session]
FAILED ...[pre_1970_extract_under_new_york_session]
FAILED ...[year_boundary_date_trunc_under_tokyo_session]
FAILED ...[year_boundary_extract_and_format_under_new_york_session]
FAILED ...[leap_day_extract_under_new_york_session]
15 failed, 16 passed in 0.73s
```

Exactly the 13 flipped rows plus the 2 re-recorded `date_trunc` rows. **The two control rows passed
under the provocation** — checked explicitly, because a provocation that reds the controls too
would prove nothing about the DATE boundary:

```text
$ pytest "...[hour_of_instant_under_new_york_session]" "...[date_extraction_is_session_zone_independent]" \
         "...[leap_day_date_arithmetic_is_session_zone_independent]" "...[date_trunc_day_across_a_zone_boundary]" -q
2 failed, 2 passed in 0.46s

E   AssertionError: date_trunc_day_across_a_zone_boundary: repark moved OFF its pinned disclosure
E   and does NOT match the recorded Spark golden either — this is a regression, not a convergence.
E   Re-derive both halves in record mode (see this module's docstring) before touching the pin. …
```

That last message is the classifier doing its job on the row whose class moved: with the fix
reverted the row matches neither half, and the runner says *regression*, not *convergence*.

**must-PASS — restored** (sha256 `3c88292d3be7f457bd569ac0098b443bcc24b4b510d2634669f8ad9d9ee69b39`
for `session_time_zone.rs`, native module rebuilt, `__pycache__` cold):

```text
$ cargo test --locked -p repark-spark --test session_timezone       -> ok. 14 passed; 0 failed
$ cargo test --locked -p repark-sql --test session_timezone_ansi_door -> ok. 2 passed; 0 failed
$ pytest python/repark/tests/test_session_timezone_parity.py -q     -> 31 passed in 0.54s
```

#### P-B2 — the DATE negative is load-bearing (the bug that actually happened)

*Claim:* the coercion path's idempotence is what keeps a `DATE` out of the session zone.
*Provocation:* restore the pre-fix arm (`Date32 | … | Utf8View => Timestamp(µs, None)`), which is
what the first working draft had.

**must-FAIL — the property, and the behavior it protects:**

```text
$ cargo test --locked -p repark-functions coercion_is_idempotent
test datetime::tests::coercion_is_idempotent_so_a_second_analysis_cannot_promote_a_date ... FAILED
assertion `left == right` failed: coerce_to_timestamp_micros is not idempotent on Date32:
a re-analysis would change the meaning of the argument
  left: Timestamp(Microsecond, None)
 right: Timestamp(Microsecond, Some("UTC"))

$ cargo test --locked -p repark-spark --test session_timezone date_arguments
test date_arguments_never_move_with_the_session_zone ... FAILED
assertion `left == right` failed: rendering a DATE under America/New_York must not shift it
  left: ["2024-02-28"]
 right: ["2024-02-29"]
```

**must-PASS — restored** (sha256 `24c416c4bdb5a23d5d0b17bcadb91e6ebed6370643f778e664acb7ed18a3eb8f`
for `datetime.rs`):

```text
$ cargo test --locked -p repark-functions coercion_is_idempotent   -> ok. 1 passed
$ cargo test --locked -p repark-spark --test session_timezone      -> ok. 14 passed
```

This unit adds no new mechanical gate (no lint entry, guard script, CI step or hook), so
`docs/testing.md` "Gate provocation proofs" does not bind it; both provocations above are here
because a green run proves nothing about detection.

### B.8 Gate output

```text
$ make ci
cargo fmt --check
cargo clippy … -D warnings                     (clean)
crate-dag: 20 internal edges clean (4 dev, 15 normal, 1 optional) across 9 of 9 mapped crates
lib-rs: 9 crate roots clean (no inline test modules; ceilings held)
lib-py: 54 files clean (ceilings held; no-stub rule held)
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
          index, the status document and the crate maps
cargo check --locked --workspace
uvx ruff@0.15.22 check .            -> All checks passed!
uvx ruff@0.15.22 format --check .   -> 241 files already formatted
uv lock --locked                    -> Resolved 29 packages in 1ms
taplo format --check / lint / typos -> clean
EXIT=0

NOTE (honest): this was the FOURTH `make ci` of the unit. Run 1 was RED at `rust-fmt-check`
(a long `strings(...)` call and a trailing blank line in the new integration file); runs 2 and 3
were RED at `rust-clippy` on `clippy::doc_markdown` — "DataFrame" without backticks, in five doc
comments across three new/edited files. Fixed, never suppressed; the backticks were NOT applied to
runtime assertion strings, where they would be noise.
```

```text
$ make test                  # cargo test --locked --workspace, never --all-features
33 `test result` lines (24 test binaries + the doc-test targets), every one `ok`;
0 `test result: FAILED`; 1306 tests passed.

Split A's tip was 31 lines / 1279 tests, counted the same way. The delta is +2 binaries
(`repark-spark/tests/session_timezone.rs` = 14 rows, `repark-sql/tests/session_timezone_ansi_door.rs`
= 2 rows) and +27 tests: those 16, plus 6 carrier pins in `repark-functions`, 2 coercion-property
pins in `datetime.rs`, 2 carrier-install pins in `repark-spark`, and 1 net elsewhere; the
extension-hook order pin was amended in place rather than added.
EXIT=0
```

```text
$ PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark/tests -q
2602 passed, 36 skipped, 46 warnings in 138.72s (0:02:18)
EXIT=0
    (run against the parity-live provisioning — the four facade extras PLUS `--extra record` — so
     the pyspark-oracle cohort actually runs rather than skipping)

$ make py-test-facade
2566 passed, 46 skipped, 37 warnings in 89.73s (0:01:29)
EXIT=0
    (this target's `uv sync` is EXACT and does NOT request `--extra record`, so pyspark is
     uninstalled and the pyspark-oracle cohort skips — the target's normal shape, and split A's
     ledger records the same 2564/46 split at its tip. +2 here: this unit's new test, and the +1
     denominator correction measured above.)
```

**The denominator, measured rather than asserted.** Split A's ledger records `2600 passed, 36
skipped` for the same command; this tree reports 2602. The difference is NOT this unit adding two
tests — it adds ONE (`test_the_extraction_class_converged_and_the_residue_is_named`). Collection is
deterministic from the file tree, so it was measured directly with `--collect-only` on the same
interpreter, with the two edited modules reverted to `HEAD` and restored by sha256 afterwards:

```text
$ pytest python/repark/tests -q --collect-only     (this tree)      -> 2638 tests collected
$ (the two edited test modules at HEAD)                             -> 2637 tests collected
```

So the pre-change tree is 2601/36 and this one is 2602/36: **+1, exactly the test this unit adds.**
Split A's recorded 2600 is one low and does not reproduce here — the same class of stale capture
its own §8.1 finding S-7 corrected once already (2593 → 2594). Recorded rather than explained away.

```text
$ JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src \
    .venv/bin/python python/repark/tests/_record_session_timezone_goldens.py
record mode: 20 rows re-derived, 0 mismatch(es)
EXIT=0

Run TWICE, as the brief asks — once against the pre-edit corpus (`git checkout HEAD --` the module,
cold `__pycache__`, restored by sha256 afterwards) and once against the edited one. **Both are
"20 rows re-derived, 0 mismatch(es)", and the AFTER delta is deliberately zero**: the driver
re-derives the SPARK half of every row, and this change edits only `repark` halves (13 replaced by
`None`, 2 re-recorded). A non-zero AFTER count would have meant a Spark golden had been touched,
which is exactly what it is there to catch.
```

```text
$ bash scripts/check_map_md.sh
map_md EXIT=0
(silent on success; the guard reads the STAGED set. Eleven touched directories' map.md were staged
alongside their code: crates/repark-core/src, crates/repark-core/src/extension,
crates/repark-functions, crates/repark-functions/src, crates/repark-functions/src/session_time_zone
(NEW dir, new map.md with a populated ## Debug), crates/repark-spark/src,
crates/repark-spark/src/extension, crates/repark-spark/tests, crates/repark-sql/tests,
crates/repark-ta/src/extension, python/repark/tests.

NOTE (honest): the FIRST run was RED — `crates/repark-spark/src/map.md` had not been updated
alongside `extension.rs`. The guard did its job; the map now describes the carrier install and
names why this door is the crossing point.)

$ python3 scripts/check_manifest.py
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc index,
the status document and the crate maps
manifest EXIT=0

$ grep -rn '"spark.sql.session.timeZone"' crates/ --include=*.rs
crates/repark-core/src/session_time_zone.rs:37   pub const SESSION_TIME_ZONE_KEY   <- the ONE key
crates/repark-functions/src/session_time_zone.rs:45  const AUTHORITATIVE_KEY       <- message text
crates/repark-functions/src/session_time_zone/tests.rs:46  (assertion)
crates/repark-spark/src/extension/tests.rs:53              (fixture)

$ grep -rn 'spark.sql.session.timeZone' python/repark/src/repark/session/session_time_zone.py
SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"                 <- the ONE facade key

One authoritative spelling per language, unchanged by this unit. The new `AUTHORITATIVE_KEY` in
`repark-functions` is **message text, never a lookup** — the crate has no `repark-core` edge, so it
cannot import the constant, and the duplicate is therefore a CHECKED MIRROR: it is asserted to equal
`repark_core::SESSION_TIME_ZONE_KEY` from the one crate that can see both
(`the_carrier_refusal_names_the_engines_own_key`). Nothing is read or written by that string.
```

### B.9 Residuals

1. **TZ-4 is the next unit, and it is bigger than it looks.** It owns `to_timestamp`'s `ns`, the
   missing `UTC` annotation, `CAST(TIMESTAMP AS STRING)`'s `string_view` + ISO-`T` rendering
   (registry queue B-TZ-4), and the Iceberg `timestamp` / `timestamptz` column choice on the write
   path. Closing it also retires TZ-6 and makes the extraction rule purely type-driven.
2. **TZ-6 is a disclosed cost, not a discovery to celebrate.** repark cannot distinguish
   `TIMESTAMP` from `TIMESTAMP_NTZ`; the fix made that observable rather than creating it, and the
   row says so. The Spark half is a *documented* value claim under registry §1's exception and
   names the unit expected to attach a real oracle.
3. **Live-scenario follow-on, deliberately not taken here.** The 13 converged rows are now
   candidates for the live tier (`repark == pinned golden == live Spark` under a non-UTC oracle),
   which would give the class drift detection on every nightly run instead of a recorded snapshot.
   That grows the registry's size pin, and the brief says size pins move only if the fix forces
   them. It does not. Whoever takes it should take the two `date_trunc` rows as live **disclosures**
   at the same time.
4. **`SET spark.sql.session.timeZone` still does not work through the SQL door** (registry queue
   B-TZ-5), and D-B2 records that making it work was possible in this unit and declined on scope.
   That decision should be revisited by whoever rules on `spark.*` conf namespacing as a whole.
5. ~~**`invoke_local_micros`'s spring-forward handling is approximate at the margin.** A
   nonexistent local time is pushed forward in 15-minute steps up to two hours, which covers every
   gap in the IANA database (all are one hour)…~~ **CORRECTED AND CLOSED in the 2026-08-10 rework
   (§B.10 N-1).** The parenthetical was factually wrong — `Australia/Lord_Howe`'s gap is 30 minutes,
   `Pacific/Apia` 2011-12-30 is 24 hours (the old two-hour bound gave up and returned NULL), and
   `Africa/Monrovia` 1972-01-07 is 44m30s (not a multiple of 15 minutes, so the old search
   overshot). The search is gone: `offset_before_gap` reads the offset in force before the
   transition and the gap resolves to `local − offsetBefore`, which is algebraically what
   `java.time` computes. What remains bounded, and is now stated as a bound rather than as a fact
   about the database, is a 26-hour lookback for that offset.
6. **The `chrono-tz` declaration is now in two crates** (`repark-core` for validation,
   `repark-functions` for resolution). Both are real enablers of the feature and both say why
   inline; `Cargo.lock` is byte-unchanged, because the feature was already resolved and only its
   ownership moved.
7. **`repark-sql` gained no new dependency for its ANSI-door cell.** The fixtures build their
   instants with arrow's own string→timestamp cast rather than a date library, so the test binary
   cannot disagree with the engine about what `2024-06-15T12:00:00Z` means and `Cargo.lock` did not
   move. The same idiom was adopted in the `repark-spark` file for consistency.
8. **The registry is high-traffic — rebase before handoff.** This change edits
   `docs/spark-sql-iceberg-parity.md` §1, §4 (TZ-3), §7 (TZ-1 retirement, TZ-4, the new TZ-6) and
   `STATUS.md`'s known-issues list, all of which other campaign units also write into. Rebase onto
   the merged tip and re-read those sections before assembly; the split-A assembly note (§9) records
   three both-added conflicts from exactly this cause.

---

---

## § Split B — REWORK after the adversarial panel (2026-08-10)

Two full-panel lenses (mechanism/semantics, corpus/registry) returned **REJECT** on the first pass
of split B. Both worked from live PySpark 4.1.2 rather than from memory, and between them raised
3 BLOCKERs, 4 MAJORs and 5 NITs. This section is what changed, why, and the evidence.

**The one sentence that explains all three BLOCKERs.** The first pass had one true idea — a repark
`TIMESTAMP` is an instant, so resolve its calendar in the session zone — and applied it as if
"tz-naive timestamp" meant one thing. It means three: an instant that lost its annotation, a
`DATE`/string promoted to a local wall clock by `date_trunc` itself, and a zoneless input that
Spark would have read as a wall clock. Reading all three as UTC instants fixed the first and broke
the other two. The rework separates them by *provenance* where it can (a new `LocalSource`) and
**declares** the one place it cannot (a zoneless input is byte-identical to a zone-suffixed one).

### B.10 Panel finding → action

| # | Sev | Finding | Action | Evidence |
|---|---|---|---|---|
| **B1** | BLOCKER | `date_trunc` across a DST fall-back returned the wrong instant and collapsed the repeated hour's two instants onto one; the doc comment's Spark-semantics rationale was factually wrong | `micros_from_local_datetime` now models `java.time.ZonedDateTime.ofLocal(local, zone, preferred)` arm for arm — **ambiguous → prefer the SOURCE instant's offset**, which is what `ZonedDateTime.truncatedTo` passes. Doc comment rewritten to the measured truth | live Spark: `('minute', 06:30:40Z)`→`06:30Z`, `('hour', 05:30Z/06:30Z)`→`05:00Z`/`06:00Z`, `('day', 06:30Z)`→`04:00Z`. Pin `date_trunc_preserves_the_source_offset_across_a_fall_back` + facade row `[date_trunc_across_the_fall_back_hour_under_new_york_session]`. must-FAIL proven (P-B3) |
| **B2** | BLOCKER | `date_trunc` of a `DATE`/string wrote LOCAL wall-clock ticks under a tz-naive type; the next extractor read that same type as a UTC instant, so the whole calendar day moved. The `DATE` negative's claim was strictly broader than its coverage | `date_trunc` now puts its truncated result on the timeline **on both paths** — Spark's `DATE`→`TIMESTAMP` promotion is a session-zone localization, so the output has ONE meaning everywhere. `LocalSource` replaced `Option<Tz>` so the two provenances are named rather than inferred. The `DATE` negative's claim was narrowed to what it covers, and the promotion got its own composed pin | live Spark: `date_trunc('day', DATE '2024-01-01')` = `2024-01-01T05:00Z` (NY) / `2023-12-31T15:00Z` (Tokyo); `year|month|dayofmonth|hour` = `2024,1,1,0` and `date_format` = `'2024-01-01 00:00'` in BOTH zones. Pin `date_trunc_of_a_date_or_string_lands_on_the_session_zone_timeline` (DATE **and** string legs, both zones) + facade rows `[date_trunc_of_a_date_composed_under_new_york_session]`, `[date_trunc_of_a_string_composed_under_tokyo_session]`. must-FAIL proven (P-B3) |
| **B3** | BLOCKER | Every zoneless TIMESTAMP input regressed from agreeing with Spark to disagreeing, undisclosed; the corpus was structurally blind (all 20 rows `…Z`-suffixed) and STATUS/TZ-1 declared the class FIXED on that basis | **Decided on measurement, outcome (b): PARTIAL.** See §B.11 for the measurement and why (a) re-opens TZ-4. The class is now declared: registry row **TZ-7**, STATUS says PARTIALLY FIXED and names the remainder, and **TZ-1 CONVERTS** to TZ-7 + TZ-8 instead of retiring | live Spark vs this tree, both zones, four spellings: §B.11's table. Pin `a_zoneless_timestamp_input_is_read_as_utc_and_diverges_from_spark` + facade rows `[zoneless_timestamp_literal_under_new_york_session]`, `[zoneless_timestamp_input_spellings_under_tokyo_session]`, `[naive_datetime_column_under_new_york_session]` — all recorded from the live oracle |
| **M1** | MAJOR | `SessionExtension` is FROZEN by a live design doc requiring a superseding note; none was written and the freeze was never named | Wrote [`docs/design/session-extension-conf-seam.md`](../docs/design/session-extension-conf-seam.md) (dated 2026-08-10), following the freeze's own amendment rule and `docs/design/map.md`'s "new dated pass, not an in-place edit". Both freeze sites now point at it; `session-api.md`'s stale signature line is corrected; D-B6's risk note names the freeze | The note prices the break (3 in-tree implementors, 0 external) and records the two rejected alternatives, including the defaulted-second-hook option and why it was refused |
| **M2** | MAJOR | The class was fixed for six functions while `to_date` / `CAST(ts AS DATE)` / `trunc` / `last_day` / `date_add` still read the stored zone | **Measured each, then split on OWNERSHIP.** `trunc` and `add_months` reach the date through this repo's own `coerce_to_date32` + invoke → **FIXED** (plus `add_months` and `datediff`, which the panel did not list, were measured too). `to_date` / `datediff` are `datafusion-spark`'s and `CAST(ts AS DATE)` is arrow's cast kernel → **DECLARED** as registry row TZ-8, pinned with repark's answer beside Spark's | live Spark NY: `trunc(…,'MM')`=`2024-05-01`, `add_months(…,1)`=`2024-06-30`, `to_date`=`2024-06-14`, `CAST AS DATE`=`2024-06-14`, `datediff`=`13`, `last_day`=`2024-05-31`, `date_add`=`2024-06-01`. Pins `date_valued_shims_take_the_date_in_the_session_zone` (fixed half) and `timestamp_to_date_paths_outside_this_crate_still_read_the_stored_zone` (declared half) |
| **M3** | MAJOR | Three statements outside the diff still said extraction does not honor the zone — including the shipped facade docstring | All three rewritten in lockstep, and all three now state the *partial* truth (what is honored, plus TZ-7 and TZ-8): `python/repark/src/repark/session/session_time_zone.py`, `crates/repark-core/src/session_time_zone.rs`, `python/repark/tests/_live_parity.py` | `grep -rn "extraction does not honor\|does not honor it yet"` → 0 hits. The facade one is user-visible, so `python/repark/src/repark/session/map.md` now records it as a standing lockstep obligation |
| **M4** | MAJOR | TZ-6 was admitted under §1's documented-value exception, but the unit *ships the oracle that measures it* — and when measured the claim is wrong (the divergence is not confined to NTZ) | **Re-recorded from the live oracle.** New corpus row declares `TimestampType` beside `TimestampNTZType` explicitly and pins both halves; the row's behavioural claim is now the measured one; the retiring unit is named (the TIMESTAMP-representation unit that closes TZ-4), and the value divergence on plain LTZ moved to TZ-7 where a user would actually look | live Spark NY: `ltz` = `timestamp[us, tz=UTC]` @ `2024-06-15T16:00Z`, `ntz` = `timestamp[us]` @ `12:00`, `hour` = 12 from both. repark: both `timestamp[us]` @ `12:00`, `hour` = 8 from both. Row `[timestamp_ntz_is_indistinguishable_from_timestamp]`, basis **recorded** |
| **M5** | MAJOR | The facade's DataFrame-API cell was pinned by a Rust `expr_fn` proxy; `df.select(F.year(...))` — the most-used spelling — was never asserted | Corpus gained an `entry_point` axis and `dataframe_api_extraction`, spelled ONCE and run on both engines (`_functions_module` resolves `F` per engine). Two rows, both non-UTC zones, `F.year` + `F.hour` + `F.date_format` + `F.date_trunc`, value AND Arrow type | live Spark NY `y=[2023,2024] h=[23,8] f=['2023-12-31 23:30','2024-06-15 08:00']`, Tokyo `y=[2024,2024] h=[13,21]`; repark's values match, and the residue is `date_trunc`'s Arrow type (TZ-4) — so the rows land as disclosures naming that class |
| **N-1** | NIT | "Gaps are an hour in every zone in the IANA database" is false | The bounded 15-minute forward search is **gone**; `offset_before_gap` computes the answer. The comment now names Lord Howe (30 min), Apia (24 h, which the old bound turned into NULL) and Monrovia (44m30s, which the old step size overshot), and states the one thing that IS bounded — a 26-hour lookback — as a bound | Both realistically reachable zones re-measured against live Spark and unchanged: Lord Howe `date_trunc('hour')` → `2024-10-05T15:30Z`, Santiago `date_trunc('day')` → `2024-09-08T04:00Z`. Pin `dst_gap_zones_resolve_like_spark` |
| **N-2** | NIT | The empty `entries()` also erases the zone from `ScalarFunctionExpr` equality; nowhere stated | Recorded in `session_time_zone.rs` beside the `entries()` rationale, in `crates/repark-functions/src/map.md`, and as risk row R-B1 below | — |
| **N-3** | NIT | "8 families + 6 negatives" — the list has 7 items and one is not a negative | Corrected to "6 boundary negatives + 1 interpretation pin", with the ledger's own P-B1 test stated as the discriminator (§B.4) | `a_tz_naive_timestamp_is_read_as_a_utc_instant` reds under P-B1; the 6 negatives do not |
| **N-4** | NIT | DIVERGENCE-1's corrected docstring names classes its pin structurally cannot observe | Docstring now states the limit outright — the pin is **UTC-only by construction**, it detects exactly one thing (a `CAST` starting to move ticks), and the classes it names are pinned under two non-UTC sessions next door. A non-UTC leg was considered and declined, with the reason given | `test_divergence_timestamp_ltz_collect_passthrough` unchanged and green |
| **N-5** | NIT | The g5 ledger's DIVERGENCE-1 carve-out row had no dated closure line | Added to `task/g5-sweep-ledger.md` — both the blind-spots paragraph and the triage row — and registry §1 now points at that home | Registry §6 designates the g5 ledger as the home; it now reads as closed there, not only in §1 |

### B.11 D-B7 — the naive-input family: measured, then DECLARED (the B3 decision)

The orchestrator's disposition admitted two outcomes and required the choice to be made on
measurement. It was.

**What was measured** (2026-08-10, this tree vs live PySpark 4.1.2, same session zone both sides):

```text
SELECT TIMESTAMP '2024-06-15 12:00:00' AS a, to_timestamp('2024-06-15 12:00:00') AS b,
       CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS c, to_timestamp('2024-06-15T12:00:00Z') AS d

repark:  a,b,c,d all timestamp[ns], all holding 2024-06-15 12:00:00 — IDENTICAL type, IDENTICAL ticks
```

`d` is a genuine instant and `a`/`b`/`c` are wall clocks Spark would have localized, and **nothing
downstream can tell them apart**. That is the whole decision. Extraction sees one type and one tick
value; any rule it applies is right for one half of the family and wrong for the other. Closing the
input half means literal planning, `to_timestamp` and `CAST` must emit a type that says which one
they made — i.e. repark's TIMESTAMP *representation*, which is registry row TZ-4's unit, whose blast
radius (D-B4) is every facade test asserting a timestamp type and the Iceberg `timestamp` /
`timestamptz` write choice. **So outcome (a) genuinely re-opens TZ-4, and the honest landing is
outcome (b).**

The consequence, measured on both sides:

| | New_York | | Asia/Tokyo | |
|---|---|---|---|---|
| | **Spark** | **repark** | **Spark** | **repark** |
| `hour(TIMESTAMP '2024-06-15 12:00:00')` | 12 | **8** | 12 | **21** |
| `hour(to_timestamp('2024-06-15 12:00:00'))` | 12 | **8** | 12 | **21** |
| `hour(CAST('2024-06-15 12:00:00' AS TIMESTAMP))` | 12 | **8** | 12 | **21** |
| `year, dayofmonth(TIMESTAMP '2024-01-01 00:30:00')` | 2024, 1 | **2023, 31** | 2024, 1 | 2024, 1 |
| naive-`datetime` column: `hour`, `year` | [0, 12], [2024, 2024] | **[19, 8], [2023, 2024]** | — | — |
| `hour(to_timestamp('2024-06-15T12:00:00Z'))` *(control)* | 8 | 8 | 21 | 21 |

**What outcome (b) obliged, all of it done:**

1. **Extraction is claimed for instant-typed inputs only.** STATUS.md says **PARTIALLY FIXED** and
   names the remainder; the module docs of `datetime.rs`, `repark-core::session_time_zone` and the
   shipped facade docstring say the same thing in the same words.
2. **The family is DECLARED with a *recorded* basis**, not a documented one: registry row **TZ-7**,
   three corpus rows, all re-derived by the committed record driver against live Spark.
3. **TZ-1 CONVERTS rather than retires.** Its registry block now says CLOSED IN PART and routes a
   reader to TZ-7 (input half) and TZ-8 (timestamp→DATE half). A reader arriving from a wrong wall
   clock is never told the class is shut.
4. **The flipped equality rows still hold, and the double-shift trap was checked**: a `…Z`-suffixed
   parse is an instant, not a local, so it must NOT be localized. It is not — the control row above
   and all 13 flips are green, and the record driver re-derives every Spark half with 0 mismatches.
5. **No previously-correct family regressed.** The rework changed three behaviors (fall-back
   truncation, `date_trunc` of a `DATE`/string, `trunc`/`add_months` over a TIMESTAMP); each is a
   convergence onto a measured live-Spark value, each has a pin, and each is proven must-FAIL
   against the pre-rework code by P-B3.

### B.12 Rework provocation proofs (verbatim)

Neither provocation is committed; both were restored from a pre-provocation copy and verified by
**sha256** — `datetime.rs` back to `5e9dd8396745284571d52522a0240ff2b4e30ddcd15f539463f772e0de434026`,
`session_time_zone.rs` back to `a324a4ee56cf6363d4003b25e50ae152df37839e72cff5356ad52bb5aa8b517f`,
and the rebuilt `_native.abi3.so` back to its exact pre-provocation
`6d50f5e27ba71b7f8267bd30b1f7ec5724b22b93f2e666545095a15fde217a17` (maturin *is* byte-reproducible
from identical sources here — the one earlier hash change in this session is fully accounted for by
doc-comment edits landing in `repark-core` / `repark-functions` between two builds). All `*.pyc`
under `python/` were deleted before every leg.

#### P-B3 (NEW) — the rework's own pins must red against the PRE-REWORK code

*The provocation is the three semantic changes, replanted exactly as the first pass had them:*
(1) `micros_from_local_datetime` ignores the preferred offset and always takes the earliest;
(2) `date_trunc`'s zone-free arm writes naive wall-clock ticks back;
(3) `coerce_to_date32` maps a `Timestamp` straight to `Date32`.

```text
$ cargo test --locked -p repark-spark --test session_timezone
test date_trunc_of_a_date_or_string_lands_on_the_session_zone_timeline ... FAILED
  assertion failed: under America/New_York Spark promotes the DATE to local midnight's INSTANT
    left: [1704067200000000]        right: [1704085200000000]     (a 5-hour, whole-day error)
test date_trunc_preserves_the_source_offset_across_a_fall_back ... FAILED
  assertion failed: truncating to the minute must not move an instant across the DST offset …
    left: [1730611800000000, 1730611800000000]                     (the repeated hour COLLAPSED)
   right: [1730611800000000, 1730615400000000]
test date_valued_shims_take_the_date_in_the_session_zone ... FAILED
  assertion failed: the instant is 2024-05-31 23:00 EDT, so its month starts in MAY
    left: [19875]                   right: [19844]                (2024-06-01 vs 2024-05-01)

test result: FAILED. 17 passed; 3 failed
```

```text
$ pytest python/repark/tests/test_session_timezone_parity.py -q     (native module rebuilt)
FAILED …[date_trunc_of_a_date_composed_under_new_york_session]
FAILED …[date_trunc_of_a_string_composed_under_tokyo_session]
FAILED …[date_trunc_across_the_fall_back_hour_under_new_york_session]
3 failed, 37 passed
```

**Exactly the rework's new BLOCKER pins red, and nothing else does** — including all 20 pre-rework
rows, the three TZ-7 rows, the TZ-6 row and both `DataFrame`-API rows, which is the check that the
rework did not quietly re-derive its expectations from its own code.

**must-PASS — restored:** `20 passed` (Rust) / `40 passed` (facade), hashes as above.

#### P-B1 and P-B2 — RE-CAPTURED on the reworked tree

`P-B1` (make the carrier reader return the default unconditionally) and `P-B2` (replant the
non-idempotent `Date32 → Timestamp(µs, None)` coercion arm) were re-run in full, because the tree
they were first captured on no longer exists.

```text
P-B1 $ cargo test --locked -p repark-spark --test session_timezone
     test result: FAILED. 5 passed; 15 failed          (was 4 passed / 10 failed before the rework)
     GREEN, correctly: the_carrier_refusal_names_the_engines_own_key,
       default_session_extracts_in_the_core_default_zone,
       time_arguments_never_move_with_the_session_zone,
       date_arguments_never_move_with_the_session_zone,
       timestamp_to_date_paths_outside_this_crate_still_read_the_stored_zone
     — the 4 original boundary negatives plus the new DECLARED-divergence pin, none of which is an
       extraction claim. Every added extraction pin reds, including
       a_zoneless_timestamp_input_is_read_as_utc_and_diverges_from_spark (it pins repark's SHIFTED
       answer, which only exists when the zone reaches the extractor).

P-B1 $ cargo test --locked -p repark-sql --test session_timezone_ansi_door
     test result: FAILED. 1 passed; 1 failed          (unchanged)

P-B1 $ pytest python/repark/tests/test_session_timezone_parity.py -q   (native module rebuilt)
     21 failed, 19 passed                              (was 15 failed / 16 passed)
     = the 13 flips + the 2 re-recorded date_trunc rows + the 3 TZ-7 rows + the TZ-6 row
       + the 2 DataFrame-API rows.

P-B2 $ cargo test --locked -p repark-functions coercion_is_idempotent
     test datetime::tests::coercion_is_idempotent_so_a_second_analysis_cannot_promote_a_date … FAILED
       left: Timestamp(Microsecond, None)   right: Timestamp(Microsecond, Some("UTC"))
P-B2 $ cargo test --locked -p repark-spark --test session_timezone date_arguments
     test date_arguments_never_move_with_the_session_zone … FAILED
       left: ["2024-02-28"]                right: ["2024-02-29"]
```

**The two provocations are complementary, and the rework made that visible.** P-B1 breaks the
*carrier*, so the zone reaches nothing and the whole extraction family reds — but the three new
composition/fall-back rows stay GREEN under it, because at the default `UTC` zone the wrong and the
right implementations agree. Only P-B3 reds them. A single provocation would have been evidence for
less than it appeared to be.

**must-PASS — restored:** `1 passed` / `20 passed` / `2 passed` / `40 passed`, all hashes as above.

### B.13 Rework gates

| Gate | Result |
|---|---|
| `make ci` | **EXIT 0** — RED four times first (rustfmt ×1; `clippy::map_unwrap_or` ×1; ruff `RUF100` unused-`noqa` + `E501` ×2; ruff-format ×1), all in new code, all fixed, none suppressed |
| `make test` (`cargo test --locked --workspace`) | **EXIT 0** — 33 `test result: ok` lines, 0 FAILED, **1312 passed** (was 1306; +6 = the six Rust rows §B.10 adds) |
| full facade suite (record provisioning) | **2611 passed, 36 skipped** (was 2602/36; **+9** = exactly the nine corpus rows added, no other test moved) |
| record driver, BEFORE row set (the 20 pre-rework rows, on this tree) | **20 rows re-derived, 0 mismatch(es)**, EXIT 0 |
| record driver, AFTER (all rows) | **29 rows re-derived, 0 mismatch(es)**, EXIT 0 — every Spark half of every NEW row reproduces bit-for-bit from live PySpark 4.1.2, and **no pre-existing Spark half was touched** (a moved one could not pass a live re-derivation without being a genuine re-record, and none is claimed) |
| P-B3 (new) | must-FAIL 3 Rust + 3 facade rows, must-PASS restored, sha256-verified — §B.12 |
| P-B1 / P-B2 re-captured | 15+1 / 21 facade / both P-B2 legs red; restored, sha256-verified — §B.12 |
| `bash scripts/check_map_md.sh` | **EXIT 0**, twice consecutively — RED once (`python/repark/src/repark/session/map.md`, whose product docstring the rework edits); 14 directories' maps staged with their code |
| `python3 scripts/check_manifest.py` | **EXIT 0**, twice consecutively |
| one-spelling conf grep | unchanged — one authoritative literal per language; the rework adds no conf key and no second spelling |

```text
$ grep -rn '"spark.sql.session.timeZone"' crates/ --include=*.rs
crates/repark-core/src/session_time_zone.rs      pub const SESSION_TIME_ZONE_KEY   <- the ONE key
crates/repark-functions/src/session_time_zone.rs const AUTHORITATIVE_KEY           <- message text
crates/repark-functions/src/session_time_zone/tests.rs   (assertion)
crates/repark-spark/src/extension/tests.rs               (fixture)

$ grep -rn 'spark.sql.session.timeZone' python/repark/src/repark/session/session_time_zone.py
SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"                 <- the ONE facade key

$ grep -rn 'datafusion.execution.time_zone' crates/ python/repark/src --include=*.rs --include=*.py
(no product-code hits — D-B2 stands)
```

### B.14 Rework risk rows and residuals

| # | Risk / residual | Disposition |
|---|---|---|
| **R-B1** | The carrier's empty `entries()` erases the session zone from `ScalarFunctionExpr` equality, so `year(ts)` built under two different session zones compares EQUAL | **Safe today, unsafe on a stated trigger.** repark has no plan cache and no cross-session expression reuse; the day either lands, return one non-settable descriptive entry and keep `set`'s refusal as the one-spelling gate. Written beside the `entries()` rationale and in the crate map, not only here |
| **R-B2** | `offset_before_gap`'s 26-hour lookback assumes no zone has two offset transitions inside one 26-hour window | True throughout the IANA database today (the widest single jump, `Pacific/Apia` 2011-12-30, is a lone 24-hour gap). Stated as a bound in the code, not as a fact about the database. Reachable only when a truncated local anchor lands inside a gap |
| **R-B3** | `SessionBuildConf` is not `#[non_exhaustive]`, so adding a field to it is itself a breaking change | Deliberate — the point of widening the argument was to make the dependency visible. The revisit trigger is written into the superseding design note |
| **R-B4** | The corpus size pins moved (G1 10–14 → 19+1, G16 6–8 → 10) | A size pin moved because the fix FORCED it: the panel measured three wrong-answer families the original 20 rows were structurally blind to. Both counts are pinned with the reason in the test's own docstring, so the next growth is a decision, not a drift |
| **R-B5** | TZ-7 and TZ-8 are new BACKLOG rows this unit opens rather than closes | Both are honest narrowings of TZ-1 rather than new discoveries: TZ-7 is the half of TZ-1 the fix did not reach (and made worse), TZ-8 is the half it never claimed. Both retire with the TIMESTAMP-representation unit that closes TZ-4, which is now named in three places |
| **R-B6** | A DST-gap *string* argument to `date_format` renders the pre-shift wall clock where Spark's promotion would shift it forward | Edge of an edge — a zoneless string whose spelled time falls inside a gap. Not pinned, and it belongs to TZ-7's family (a zoneless input repark cannot localize), not to a separate defect |

### B.15 What the panel got right that did NOT change

Recorded because a rework that only lists its own edits reads as if everything was wrong. Both
lenses reproduced, independently and against live Spark, the parts of the first pass that hold:
D-B2's rejection of `datafusion.execution.time_zone` (the option does not drive `date_part`); the
carrier being unreachable from `SET` through both doors, verified in the vendored `ConfigOptions`
source and empirically; invoke-time reading being correctly plumbed through `ConstEvaluator` on the
folding path and `SessionState` on the column path; the coercion path being idempotent on all
twelve input types; re-annotation being metadata-only across all four `TimeUnit`s; the flip
inventory being exactly right row-for-row with 0 Spark halves moved; the LOCATION RULE honored; and
the denominator correction being measured rather than asserted. **The seam design was not the
problem — the boundary of the claim was**, and that is what this rework moved.

## 9. Assembly note (orchestrator, 2026-08-10)

Landed after H-1d merged (#35). Rebase onto its squash produced three both-added conflicts
(`test_parity_live.py` — the mirror gate beside this unit's two new pins; the two `map.md`
I-want-to tables), all resolved by keeping both sides.

**Registry placement applied the registry's own class vocabulary, not this ledger's §6 table
heading** ("Declared-divergence rows" was loose): TZ-2 and TZ-3 are DECLARED and landed in §4;
TZ-1, TZ-4 and TZ-5 carry intent-to-fix and landed in §7 as BACKLOG rows, whose pins codify
today's behavior so the fix reds them — exactly this unit's disclosure design. B-TZ-1…B-TZ-5
carry **no pin**, so under the registry's §6 admission rule they are not rows: they landed as
§7's "Surfaced, awaiting pins" queue, each pointing back at this ledger for the full observed
behavior. STATUS gained two state lines (the TZ-1/TZ-4 class with split B in flight; TZ-5 open,
own unit) per the state-vs-semantics boundary.
