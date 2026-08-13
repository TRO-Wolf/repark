# H-1b — the Spark door's time-travel ephemeral-view leak

> **ARCHIVED 2026-08-11** (G-9 — H-1 phase ledger promotion) — a historical record of everything
> delivered through the H-1 close gate (repark #35–#46), including the parallel G/N corpus units
> whose gap-map homes are H-2, kept for provenance and **not a source of live rules**: every rule
> still in force was verified live-elsewhere or promoted first
> ([promotion-ledger.md](promotion-ledger.md)). Relative links were repaired for this location on
> the same date; nothing else changed. Current state: [STATUS.md](../../../STATUS.md).

**Campaign:** V2 Engine Hardening. **Unit:** H-1b. **Date:** 2026-08-11.
**Branch:** `hardening/h1b-tt-leaks`, based at `e239177`.

## 1. What this unit is, and where its provenance hangs

H-1b **closes an open row that v2 already carried in writing** — it does not open a new one.
The row is [`../docs/history/port-v2/p2g-ansi-m2-ledger.md`](../port-v2/p2g-ansi-m2-ledger.md)
"Riders carried forward" item 4:

> *"NEW, from the verify pass — the SPARK door has the same time-travel view leak.
> `crates/repark-spark/src/time_travel.rs` registers `__repark_tt_<n>` and never deregisters it,
> exactly as the ANSI door did (finding 3 above). … The ANSI-side fix is the template
> (`PinnedViews` + release after planning). File it as the first rider of the next Spark-door
> unit."*

That rider is now marked CLOSED in the same ledger, and finding 3's own Fix cell — which the
rider points at as "the template" — is CORRECTED there, because the template turned out not to be
a fixed door (§4 below).

`docs/spark-sql-iceberg-parity.md` (the divergence registry) is deliberately **untouched**: this
unit fixes a defect, and a fixed defect gets no registry row (the H-1c precedent).

## 2. The defect

The Spark door's `prepare_time_travel_sql` registered one snapshot-pinned `__repark_tt_<n>` temp
view per `AS OF` relation and never deregistered it. Consequences, both real:

* **unbounded accumulation** — one registration per AS OF relation per query, on a session that
  may live for hours;
* **user-visible rows in the introspection surface** (`SHOW TABLES` /
  `information_schema.tables`) after a statement that SUCCEEDED *and* after one that FAILED — the
  rewrite registers before the plan can fail, so a failing statement leaked too, at two depths
  (mid-rewrite, and post-rewrite planning failure).

## 3. The fix — three production hunks, re-ported by MEANING

Ported from **the private v1 repository's shipped fix** (`d146496`, read only) and from v2's own
ANSI door, which supplied the structural shape. **Not a `git apply`:** the `Cow` juggling differs
(the moved Spark code borrows its `sql` parameter, where v1's consumed the owned `sql_after_meta`),
the Spark door takes `(ctx, catalogs)` positionally where the ANSI door threads an
`EngineContext<'_>`, and the Spark door registers an `Arc<IcebergStaticTableProvider>` inline
where the ANSI door registers `frame.into_view()` through a helper.

| Hunk | File | Content |
|---|---|---|
| A | `crates/repark-spark/src/time_travel.rs` | `pub struct PinnedViews { names: Vec<String> }` + best-effort `release(&self, ctx)`. Doc states the registration only has to survive PLANNING (the resolved `TableScan` owns the provider), and carries the engine-reserved-`__repark`-prefix note. |
| B | same file, inside `for span in spans.into_iter().rev()` | `pinned.names.push(temp_name.clone());` **before** `ctx.register_table(…)`, so (a) a registration that fails after taking the name is still drained and (b) every later `?` in the loop releases what earlier turns took. Signature gains `pinned: &mut PinnedViews` as the last parameter. *(Rationale (a) narrowed 2026-08-11 — see §11.4: it is DEFENSIVE-only. (b) is the load-bearing half and is what the pins actually detect.)* |
| C | `crates/repark-spark/src/router.rs` | New private `async fn execute_time_travelled(ctx, catalogs, sql: &str, pinned: &mut PinnedViews) -> Result<DataFrame>` holding the TT rewrite + `execute_inner` call. `execute_with_read_only` constructs `PinnedViews::default()`, binds the call's `Result` **without** `?`, calls `pinned.release(ctx)`, returns the bound result. |

### Wording — deliberate, and corrected relative to the ANSI door

The claim is **"every `?` / `return` path"**, never "EVERY exit path". Unwind and future-drop
still bypass the release: `PinnedViews` deliberately carries no `Drop` impl (it would have to own
a `SessionContext` clone), and neither source exists today — panics are banned in prod code and
the PyO3 facade drives this via `block_on`. **Decision D-1: v2 does NOT take the `Drop`.** Adding
it would mean cloning a `SessionContext` into every statement's ledger to buy a guarantee against
two sources that do not exist; the honest scope statement is cheaper and is now written down in
three places (the router comment, `crates/repark-spark/src/map.md`, and the corrected p2g row).
The same over-claim in v2's ANSI door (`crates/repark-sql/src/router.rs` and the p2g ledger row)
is corrected in this diff.
*(Count reconciled 2026-08-11 — **four**, not three: the ANSI router's own wording was NOT
corrected in the delivery, only claimed to be. F-2 fixed it; see §11.2.)*

## 4. §4 — the ANSI door, the "template", was itself still leaking

The re-port map's CLAIM 3, **confirmed here empirically on this tree**. `register_pinned_view`
composes its `__repark_ansi_tt_<n>` view over `repark_core::read_table_at`, which registers a
`__repark_tt_<n>` of its own before returning the frame. Only the ANSI-prefixed name went into
`PinnedViews`, and the pin filtered `LIKE '__repark_ansi_tt%'` **only** — so the composed half
escaped both the ledger and its own regression test, on the door whose ledger row declared the
leak fixed.

**Fixed here (map §4 Option 1), inside `register_pinned_view`, entirely within
`crates/repark-sql/src/time_travel.rs`.** The core-minted name is read off the returned frame's
logical plan (`LogicalPlan::TableScan` → `table_name.table()`, prefix-checked) and recorded in the
SAME `PinnedViews`, before `frame.into_view()` consumes the frame.

> **RATIONALE CORRECTED 2026-08-11 by the panel fix pass — read §11.3 before this block.** The
> ordering constraint asserted below ("an immediate deregister sits *after* the
> `register_table(…)?` call") does NOT exist: the map's own wording puts the deregister after
> `into_view()` and *before* `register_table`, which has no error-path hole. The shipped route is
> still the right one, for different (real) reasons; the alternative was viable. §11.3 states both.
>
> **DEVIATION D-2 (flagged, not absorbed).** The map words Option 1 as *"deregister the
> core-minted name inside `register_pinned_view` once `frame.into_view()` has been taken"*. This
> implementation keeps Option 1's **location and blast radius** exactly — one function, one crate,
> `read_table_at` untouched, the reader-options caller untouched by construction — but releases
> via the **ledger** instead of an immediate `deregister_table`. Rationale: an immediate
> deregister sits *after* the `register_table(…)?` call, so a registration failure would return
> `Err` with the core name still registered — reintroducing, on the error path, exactly the class
> of bug this unit exists to remove. The ledger route drains it on every `?` / `return` path of
> the same split that already drains the ANSI name, and the release timing is *later*, not
> earlier, than the map's wording — i.e. strictly safer with respect to the "must survive
> planning" argument (the `ViewTable` already owns the resolved `TableScan`).
> **Residual of the residual:** if `read_table_at` itself fails *after* its `register_table`
> succeeds (its `ctx.table(…)` lookup), the name is unobservable from this side and would leak.
> That path requires a registration to succeed and the immediately following lookup of the same
> name to fail; closing it would require Option 2 (threading the ledger into `read_table_at`),
> whose blast radius reaches the reader-options caller. Recorded, not silently absorbed.

**Decision D-3: fix, don't defer.** Map §8.4 allowed either. It was fixed because the pin
broadening (which §8.4 mandates unconditionally) would otherwise have had to ship RED or
`#[ignore]`d — both forbidden by `docs/testing.md`.

## 5. §6 — the reader-options residual, recorded (not silently claimed fixed)

`repark_core::time_travel::read_table_at` registers a `__repark_tt_<n>` and never deregisters it.
It has two callers with **different correct dispositions**:

| Caller | Disposition |
|---|---|
| `crates/repark-core/src/session.rs` — the reader-options path (`spark.read.option("snapshot-id" \| "as-of-timestamp" \| "branch" \| "tag", …)`) | **Keep the registration.** It backs the `DataFrame` handed to the user and has no statement boundary. This is the DOCUMENTED RESIDUAL — and it is what supplies the facade pin's non-vacuity (§6 below). |
| `crates/repark-sql/src/time_travel.rs` — the ANSI door | **Was a leak.** Fixed, §4. |

Recorded in `crates/repark-core/src/map.md` (the crate that owns the code) and in
`crates/repark-spark/src/map.md` `## Debug` as producer 2 of three. **Decision D-4: the residual
is recorded, not fixed** — releasing it would break the reader-options contract, and no claim in
this diff says otherwise.

## 6. The facade landmine

`python/repark/tests/test_time_travel.py::test_time_travel_temp_views_hidden_from_list_tables`
was character-for-character v1's pre-fix version, and it **encoded the leak as its own
non-vacuity proof**:

```
assert raw_names, "expected at least one __repark_tt_* registration after time-travel read"
```

v2's facade `spark.sql` routes through the Spark door, so that line goes red under any correct
Spark-door fix. Repaired in the SAME change, by the v1 pattern — **rewritten, not deleted**:

1. the pin's real subject is kept (the `listTables` prefix filter, `python/repark/src/repark/catalog.py`);
2. the SQL rewrite is now asserted to add **nothing**: `_tt_registrations()` snapshot before and
   after a `VERSION AS OF` query, compared for equality;
3. **non-vacuity re-sourced** from the reader-options path
   (`spark.read.format("iceberg").option("snapshot-id", …)`), which still registers by design —
   `assert len(after_read) > len(before)` with a message naming both sides;
4. only then the `listTables` filter assertion, unchanged.

## 7. Gate evidence (verbatim)

### 7a. ANSI pin, RED then GREEN (map §8.4 — added first, watched red, then fixed)

`cargo test -p repark-sql --test introspection time_travel_pinned_views_do_not_leak_into_the_introspection_surface -- --test-threads=1` — **before** the §4 fix:

```
running 1 test
test time_travel_pinned_views_do_not_leak_into_the_introspection_surface ... FAILED

failures:

---- time_travel_pinned_views_do_not_leak_into_the_introspection_surface stdout ----

thread 'time_travel_pinned_views_do_not_leak_into_the_introspection_surface' (678314) panicked at crates/repark-sql/tests/introspection.rs:297:5:
the core half of each pinned relation must be released too, not left on the session: ["__repark_tt_1", "__repark_tt_2", "__repark_tt_3"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    time_travel_pinned_views_do_not_leak_into_the_introspection_surface

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 4 filtered out; finished in 0.07s

error: test failed, to rerun pass `-p repark-sql --test introspection`
```
exit `101`. The leftover set is exactly what the map predicted (`__repark_tt_1|2|3`).

`cargo test -p repark-sql --test introspection -- --test-threads=1` — **after** the fix:

```
running 5 tests
test information_schema_enumerates_an_iceberg_catalog_through_the_ansi_door ... ok
test introspection_still_refuses_without_the_information_schema_conf ... ok
test metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_ansi_door ... ok
test show_tables_and_describe_delegate_through_the_ansi_door ... ok
test time_travel_pinned_views_do_not_leak_into_the_introspection_surface ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.19s
```
exit `0`.

### 7b. The two new Spark-door pins, green on the shipped tree

`cargo test -p repark-spark --lib tests::time_travel:: -- --test-threads=1`:

```
running 3 tests
test tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement ... ok
test tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement ... ok
test tests::time_travel::time_travel_version_timestamp_branch_tag_and_errors ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 351 filtered out; finished in 0.36s
```
exit `0`.

### 7c. Mutation 1 — drop `pinned.release(ctx)` in `execute_with_read_only`

Both new pins red; the pre-existing twin stays green (it never asserted leftovers).

```
warning: method `release` is never used
  --> crates/repark-spark/src/time_travel.rs:92:12
   |
80 | impl PinnedViews {
   | ---------------- method in this implementation
...
92 |     pub fn release(&self, ctx: &SessionContext) {
   |            ^^^^^^^
   |
   = note: `#[warn(dead_code)]` (part of `#[warn(unused)]`) on by default

warning: `repark-spark` (lib test) generated 1 warning
    Finished `test` profile [unoptimized + debuginfo] target(s) in 13.04s
     Running unittests src/lib.rs (target/debug/deps/repark_spark-2aa14407d468c143)

running 3 tests
test tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement ... FAILED
test tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement ... FAILED
test tests::time_travel::time_travel_version_timestamp_branch_tag_and_errors ... ok

failures:

---- tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement stdout ----

thread 'tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement' (704571) panicked at crates/repark-spark/src/tests/time_travel.rs:367:5:
a rewrite that failed half-way must release what it already registered: ["__repark_tt_1"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement stdout ----

thread 'tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement' (704573) panicked at crates/repark-spark/src/tests/time_travel.rs:328:5:
time-travel temp views must be released, not left on the session: ["__repark_tt_2", "__repark_tt_3", "__repark_tt_4", "__repark_tt_5", "__repark_tt_6"]


failures:
    tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement
    tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement

test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 351 filtered out; finished in 0.37s

error: test failed, to rerun pass `-p repark-spark --lib`
```
exit `101`.

Note the mid-rewrite leftover is a SINGLE name (`__repark_tt_1`): the splice runs right-to-left,
so the right-hand (good) pin registers first and the left-hand (bad) one aborts before minting —
which is why the failure test puts the BAD pin on the LEFT.

### 7d. Mutation 2 — `if result.is_ok() { pinned.release(ctx); }`

The acceptance the map insists on: a fix that only survives mutation 1 has not re-ported the
error-path half. Under mutation 2 the **success pin stays green and ONLY the error-path pin
reds** — which is precisely what earns the second test its place.

```
running 3 tests
test tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement ... FAILED
test tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement ... ok
test tests::time_travel::time_travel_version_timestamp_branch_tag_and_errors ... ok

failures:

---- tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement stdout ----

thread 'tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement' (709178) panicked at crates/repark-spark/src/tests/time_travel.rs:367:5:
a rewrite that failed half-way must release what it already registered: ["__repark_tt_1"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement

test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 351 filtered out; finished in 0.36s

error: test failed, to rerun pass `-p repark-spark --lib`
```
exit `101`.

### 7e. Mutation 3 — the same drop, seen through the FACADE (the repaired pin is not a tautology)

The repaired facade test's first assertion ("the SQL rewrite adds nothing") is the half that
replaced the leak-encoding `assert raw_names`. It is a real pin: with `pinned.release(ctx)`
dropped and the native module rebuilt (`maturin develop`), it reds, naming the leaked view.

```
        s1 = multi_snapshot["s1"]
        before = _tt_registrations(spark)

        _ = spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF {s1}").to_arrow()
>       assert _tt_registrations(spark) == before, (
            "the SQL time-travel rewrite must release its ephemeral pins once the statement is planned"
        )
E       AssertionError: the SQL time-travel rewrite must release its ephemeral pins once the statement is planned
E       assert ['__repark_tt_1'] == []
E
E         Left contains one more item: '__repark_tt_1'
E         Use -v to get more diff

python/repark/tests/test_time_travel.py:320: AssertionError
=========================== short test summary info ============================
FAILED python/repark/tests/test_time_travel.py::test_time_travel_temp_views_hidden_from_list_tables
1 failed, 30 deselected in 0.71s
```
exit `1`. Restored + rebuilt → `31 passed in 7.80s`, exit `0`.

The pin's OTHER half — non-vacuity — is self-proving: `assert len(after_read) > len(before)`
passes on the shipped tree, which is the statement that the reader-options registration really is
there for `listTables` to hide (§5).

**Restore proof.** All three mutations touched `crates/repark-spark/src/router.rs` only, each
restored from a byte copy taken immediately before it, sha256-verified after each restore:

| Point | sha256 of `crates/repark-spark/src/router.rs` |
|---|---|
| Before mutation 1; after restoring from mutation 1; after restoring from mutation 2 | `1cf85e445e2176d070400a34b01504c8ff12ce42b80af7749f309b4f3727cf7f` |
| After `cargo fmt` (whitespace only — rustfmt joined the `let result = execute_time_travelled(…)` statement onto one line; no token changed) — and after restoring from mutation 3 | `334624afc2ad42a33bcab71531f80fad7538a0b7f8f049a51ce025c88e35012c` |

The formatting pass sits BETWEEN mutations 2 and 3, which is why two hashes appear; the shipped
file is the second. Mutation identifiers appear nowhere in the shipped tree. The Rust transcripts
were captured with `-- --test-threads=1`: `TEMP_VIEW_SEQ` is a process-global atomic, so under
cargo's default parallelism the `__repark_tt_<n>` NUMBERS shift run to run while the set SIZES
hold.

### 7f. `make verify` — GREEN

`make verify > verify.log 2>&1; echo "VERIFY_EXIT=$?"` → `VERIFY_EXIT=0` (real exit code, not a
pipe's).

Steps run, in order, all clean: `cargo fmt --check`; `cargo clippy --locked --workspace
--all-targets -- -D warnings -A clippy::disallowed_methods`; the two panic-ban clippy passes;
`cargo check --locked --workspace`; `check-crate-dag`; `check-lib-rs`; `check-rust-file-size`;
`check-lib-py`; `check-manifest`; `check-parity-live-dual-wire`; `uvx ruff@0.15.22 check .` +
`format --check .`; `uvx taplo@0.9.3 format --check` + `lint`; `uvx typos@1.47.2`;
`cargo test --locked --workspace`.

Test counts on the workspace run (the load-bearing one for this unit):

```
test result: ok. 354 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.83s   # repark-spark lib (352 → 354: the two new pins)
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s     # repark-sql tests/introspection.rs (the broadened leak pin)
test result: ok. 246 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 6.07s   # repark-iceberg lib
test result: ok. 208 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.36s   # repark-sql lib
```

> **First run was RED and is recorded as such:** `VERIFY_EXIT=2`, `rust-fmt-check` only — rustfmt
> wanted the new `let result = execute_time_travelled(…)` on one line. Fixed with `cargo fmt`
> (whitespace only), then re-run clean. No gate was skipped or worked around.

### 7g. `make py-test-facade` — GREEN

`make py-test-facade > facade.log 2>&1; echo "FACADE_EXIT=$?"` → `FACADE_EXIT=0`. It provisions
the four declared extras, builds the native module through maturin, and runs the facade suite
against it:

```
📦 Built wheel for abi3 Python ≥ 3.12 to /tmp/.tmpYeJbsN/repark-0.0.0-cp312-abi3-linux_x86_64.whl
✏️ Setting installed package as editable
🛠 Installed repark-0.0.0
…
2634 passed, 46 skipped, 37 warnings in 100.78s (0:01:40)
```

The repaired pin specifically, run alone against the same build:

```
python/repark/tests/test_time_travel.py .                                [100%]

======================= 1 passed, 30 deselected in 0.69s =======================
```
exit `0` — it RAN, it was not skipped.

## 8. Decisions

| # | Decision | Rationale |
|---|---|---|
| D-1 | No `Drop` impl on `PinnedViews`; the claim is worded "every `?` / `return` path" | A `Drop` would have to own a `SessionContext` clone per statement, to guard two sources that do not exist today (panics banned in prod; PyO3 drives via `block_on`). An honest scope statement is cheaper than a speculative guarantee. |
| D-2 | ANSI §4 fix records the core name in the ledger rather than deregistering immediately (**flagged deviation**, §4) | ~~An immediate deregister sits after `register_table(…)?`; a registration failure would then return `Err` with the core name still registered.~~ **Rationale corrected 2026-08-11 — see §11.3.** That constraint does not exist; the shipped route stays for its real reasons (one uniform drain with the ANSI name; releases LATER, which is the safe direction against "must survive planning"). |
| D-3 | §4 fixed in-unit rather than deferred | The mandated pin broadening would otherwise have to ship RED or `#[ignore]`d — both forbidden. |
| D-4 | The reader-options `read_table_at` registration is RECORDED as a residual, not fixed | It backs the `DataFrame` handed to the user; there is no statement boundary to release at. It is also the facade pin's non-vacuity source. |
| D-5 | The two new test helpers stay leaf-private in `tests/time_travel.rs`, not in `tests/common.rs` | `common.rs` is for cross-cutting helpers more than one leaf needs (its own map rule); these have one consumer. |
| D-6 | The Rust pins read the default catalog/schema directly, not `information_schema` | Same proof, one fewer precondition — this door's `setup` does not enable the introspection conf (the ANSI harness does, which is why the ANSI pin queries `information_schema`). |
| D-7 | `docs/spark-sql-iceberg-parity.md` untouched | H-1b closes a rider; a fixed defect gets no registry row, and the registry is orchestrator-owned. |
| D-8 | A third mutation was run through the FACADE (§7e), beyond the two the map mandates | The facade pin was REWRITTEN, not merely kept green; a rewritten pin has to be shown to still detect the thing it now claims to detect, or the repair could have quietly turned it into a tautology. |

## 9. Files changed

**Production**
- `crates/repark-spark/src/time_travel.rs` — Hunks A + B.
- `crates/repark-spark/src/router.rs` — Hunk C.
- `crates/repark-sql/src/time_travel.rs` — §4 (core-name recording + `core_pinned_name`).

**Tests**
- `crates/repark-spark/src/tests/time_travel.rs` — Hunk D (two pins + two helpers).
- `crates/repark-sql/tests/introspection.rs` — the `'__repark_tt%'` half of the ANSI pin.
- `python/repark/tests/test_time_travel.py` — the facade landmine, rewritten.

**Docs (lockstep)**
- `crates/repark-spark/src/map.md` — router row, `time_travel.rs` row (first `__repark_tt` row this
  file has ever carried), `## Debug` row + the three-producer bullet + the engine-reserved-prefix
  bullet.
- `crates/repark-spark/src/tests/map.md` — the added pins + why the helpers stay leaf-private.
- `crates/repark-sql/src/map.md` — `time_travel.rs` contents row + a second `## Debug` row.
- `crates/repark-sql/src/time_travel/map.md` — `## Debug` row for the core half.
- `crates/repark-sql/tests/map.md` — the broadened leak pin.
- `crates/repark-core/src/map.md` — the `read_table_at` residual, recorded where the code lives.
- `python/repark/src/repark/map.md` — what the `listTables` filter's live subject now is.
- `python/repark/tests/map.md` — the rewritten facade pin.
- `docs/history/port-v2/p2g-ansi-m2-ledger.md` — finding 3 corrected (both halves); rider 4 closed.
- `task/map.md` — this ledger linked.

## 10. Deviations, flagged

1. **D-2** (§4): Option 1's location, ledger release instead of immediate deregister — with the
   remaining `read_table_at`-internal failure path stated rather than papered over.
2. **The map's `:N` cites were stale, as it warned.** Everything was re-located by symbol / test
   name / grep string. The one the map flagged explicitly had indeed happened: G-4 split
   `crates/repark-spark/src/tests.rs`, so the twin
   `time_travel_version_timestamp_branch_tag_and_errors` lives at
   `crates/repark-spark/src/tests/time_travel.rs`, and the two new pins went there.
3. **Nothing was committed or pushed.** The unit is delivered as a working tree for orchestrator
   review, per the unit brief.

## 11. Panel fix pass (2026-08-11)

A two-lens adversarial panel reviewed the delivery above; the orchestrator dispositioned every
finding (F-1 … F-20, one declined). This section is the fix pass's own record. It **appends** —
nothing in §§1–10 was rewritten, and the two rationale corrections the dispositions ordered are
filed here (§11.3, §11.4) with a dated pointer left at each original so neither can be read
without its correction.

### 11.1 Finding → action

| # | Lens finding | Action taken | Where |
|---|---|---|---|
| F-1 | lens1 MAJOR-1 — two process-global `__repark_tt_` counters | **Engine fix.** Deleted `crates/repark-spark/src/time_travel.rs`'s `static TEMP_VIEW_SEQ` + its local `next_temp_view_name`. `repark_core::time_travel::next_temp_view_name` is now `pub` (banner-doc'd with WHY: the namespace is shared across crates on one `SessionContext`, so a second counter is a data-loss bug, not untidy numbering) and re-exported from `repark_core`. The Spark rewrite calls it. Names cannot collide by construction. | `crates/repark-core/src/{time_travel.rs,lib.rs}`, `crates/repark-spark/src/time_travel.rs` |
| F-1 (rider) | the now-"dead" pre-register `deregister_table` in the Spark door | **KEPT, with the reason recorded — it is not dead.** DataFusion's `MemorySchemaProvider::register_table` REFUSES a duplicate name (`datafusion-catalog-54.1.0/src/memory/schema.rs:69`, "The table {name} already exists"). Engine-minted collisions are now impossible, but a user squatting the reserved `__repark_tt_<n>` name is not: without this line the statement would FAIL instead of clobbering, which would silently change the engine-reserved-prefix behavior the map documents. | `crates/repark-spark/src/time_travel.rs` (comment at the mint site) + `crates/repark-spark/src/map.md` reserved-prefix bullet |
| F-1a | mandatory revert-red evidence | New pin `time_travel_statement_pins_never_collide_with_a_reader_options_view` + leaf-private helper `temp_view_sequence`. Registers a reader-options view via `repark_core::read_table_at` FIRST, runs a Spark-door `VERSION AS OF`, asserts the reader's registration SURVIVES and the statement's own pins are gone; then asserts the door minted from repark-core's sequence. Mutation transcripts: §11.5. | `crates/repark-spark/src/tests/time_travel.rs` |
| F-1b | falsified docs | Re-trued, each at its own site, and the single-counter/shared-namespace note added to the three-producer Debug bullet: spark map producer-2 "keeps the registration" (true only until an unrelated statement ran); core map "deliberately unchanged" (same); python map "live subject is reader-options only" (the two paths could collide); `PinnedViews::release`'s "only touches names this statement minted itself". | `crates/repark-spark/src/map.md`, `crates/repark-core/src/map.md`, `python/repark/src/repark/map.md`, `crates/repark-spark/src/time_travel.rs` |
| F-2 | lens1 MAJOR-2 = lens2 M1 — ANSI router wording | `crates/repark-sql/src/router.rs`: the call-site comment gains the unwind/future-drop caveat verbatim as the Spark copy carries it, and the `execute_time_travelled` doc says "every `?` / `return` path" instead of "EVERY exit path". D-1's "three places" is now **four** and is stated as such in §11.2. | `crates/repark-sql/src/router.rs` |
| F-3 | lens2 N1 — facade step 4 could green on an empty listing | Positive-membership assertion added BEFORE the leak assertion: `assert TABLE.rsplit(".", 1)[-1] in ns_listed`. | `python/repark/tests/test_time_travel.py` |
| F-4 | lens2 N7 — uncaptured docstring claim | **Captured** (§11.6) — and the capture showed the claim "both leftover assertions red" is not what a run can show: a panic reports ONE assertion. Docstring narrowed to the captured evidence for both mutations. | `crates/repark-sql/tests/introspection.rs` |
| F-5 | lens2 M2 — STATUS open-defect entry | Entry deleted; the dated "Closed out of this section" paragraph gains the H-1b line (pure pointer to `task/h1b-ledger.md`, zero behavior words — the H-1c `831b7a0` pattern and its own lesson 4). | `STATUS.md` |
| F-6 | lens2 M3 — port-execution-log | Open checkbox closed (`[x]`) with "Closed by H-1b (2026-08-11), see task/h1b-ledger.md" and the no-divergence-row outcome; the "Open debt" lead-in gains both closures; "every exit path" → "every `?` / `return` path". | `docs/history/port-v2/port-execution-log.md` |
| F-7 | lens2 M4 — spark crate map known-limitations row | Rewritten: both halves were stale (leak closed by H-1b, `$`-metadata rider by H-1c). The row now states both closures with their pointers and routes "what is still open" to STATUS. | `crates/repark-spark/map.md` |
| F-8 | lens2 M5 + lens1 NIT-2 — residual-of-residual stated where the fix is | Both residuals now stated at the site of the fix: (a) the `read_table_at`-internal failure between its own `register_table` and its `ctx.table` lookup, needing Option 2; (b) **the second residual** — `core_pinned_name` answering `None` silently restores the leak, because `None` is also the legitimate answer — WITH its fence (the broadened `LIKE '__repark_tt%'` pin reds on the leftover, not on the recovery). | `crates/repark-sql/src/time_travel.rs` (`core_pinned_name` doc) + the p2g correction block |
| F-9 | lens2 M6 — design doc | Dated correction under the §2.1 table: the ANSI door was NOT a fixed door (its core-minted half leaked until H-1b), the template was **structural only**, and "every exit path" overstates what either door intends. | `docs/design/v2-engine-hardening.md` |
| F-10 | lens1 MAJOR-3 — D-2's rationale is wrong | Rationale corrected in full at **§11.3**; a dated pointer left at the §4 block and in the §8 D-2 row so the wrong reason cannot be read alone. | `task/h1b-ledger.md` |
| F-11 | lens1 NIT-1 — Hunk B rationale (a) | Narrowed to defensive-only at **§11.4**; ordering KEPT; dated pointer left in the §3 hunk table. | `task/h1b-ledger.md` |
| F-12 | lens1 NIT-3 — "error message unchanged" | **Not present in this ledger or in any test comment** (verified by grep: the tests assert `.to_string().contains(<token>)` and say "must still name …"). The over-claim lives only in the planning-side actor report §2. **FLAGGED, not silently absorbed** — see §11.7. | — |
| F-13 | lens2 N2 — adjacent Debug row | "an exit path" → "a `?` / `return` path", plus the unwind/future-drop clause. | `crates/repark-sql/src/map.md` |
| F-14 | lens2 N3 — `read_table_at`'s own doc | Both callers named with their different correct dispositions; the note that `core_pinned_name` reads THIS function's plan shape + prefix, and that both are therefore load-bearing at the producing site. | `crates/repark-core/src/time_travel.rs` + `crates/repark-core/src/map.md` |
| F-15 | lens2 N4 — Debug table row vs producer-2 bullet | Row rewritten to carry the leak-vs-documented-residual caveat in the same breath ("from either SQL door it is a LEAK, from the reader-options path it is the DOCUMENTED RESIDUAL"). | `crates/repark-spark/src/map.md` |
| F-16 | lens2 N5 — nine-vs-ten count | **Not present in this ledger** (§9 lists the files without a count word). The mismatch is in the planning-side actor report (§1 row 8.6 "nine map/doc files" vs §4's ten rows). **FLAGGED** — §11.7. The true post-fix-pass file list is §11.8. | — |
| F-17 | lens2 N6 — v1 citation wording | §3 now reads "the private v1 repository's shipped fix (`d146496`, read only)". The `/tmp` path inside the §7e transcript is precedented and left verbatim. | `task/h1b-ledger.md` §3 |
| F-18 | lens2 N8 — brief | Dated **annotation** (briefs are versioned — narrowed, not rewritten): "every exit path, including the error paths" → the map's "every `?` / `return` path", with the error-path half affirmed as delivered and pinned. | `briefs/v2-engine-hardening.md` |
| F-19 | lens2 N9 — p2g rider 4 | "(see the correction to finding 3 above)" added INSIDE the closed rider, at the point where it calls the ANSI fix "the template" — so the endorsement cannot be read without the correction that the template was structural only. | `docs/history/port-v2/p2g-ansi-m2-ledger.md` |
| F-20 | lens2 N10 — promotion-ledger pointers | Both rows gain "CLOSED by H-1b (2026-08-11)" state words and re-point at this ledger, noting the STATUS entry was deleted by the fixing unit per that section's own rule. | `docs/history/port-v2/promotion-ledger.md` (~:174, ~:273) |
| — | **DECLINED** by the orchestrator: lens1 NIT-4 (the `LIKE '_'` single-char wildcard) | Not implemented, per the disposition. No producible name hits it, and no existing row absorbed a note naturally. | — |

### 11.2 D-1's "three places" is now FOUR — reconciled

§3 recorded the honest scope wording as living in "three places (the router comment,
`crates/repark-spark/src/map.md`, and the corrected p2g row)". F-2 adds the ANSI door's own router
(`crates/repark-sql/src/router.rs` — both the call-site comment and the `execute_time_travelled`
doc), which the p2g correction block already CLAIMED carried it. The count is now four sites, and
the p2g claim is true as written for the first time. `crates/repark-sql/src/map.md`'s Debug row
(F-13) and the brief's annotation (F-18) restate it as pointers, not as independent claims.

### 11.3 D-2's rationale, corrected (F-10 / lens1 MAJOR-3)

**The shipped decision does not change. Its stated reason was wrong, and that matters more than
the decision.**

The §4 block argues that the map's wording forces an unsafe ordering: *"an immediate deregister
sits **after** the `register_table(…)?` call, so a registration failure would return `Err` with the
core name still registered."* **That constraint does not exist.** The map words Option 1 as
"deregister the core-minted name inside `register_pinned_view` once `frame.into_view()` has been
taken" — i.e. AFTER `into_view()` and **BEFORE** `register_table`. Written that way there is no
error-path hole at all: by the time `register_table` can fail, the core name is already gone. The
map-faithful shape was **viable**, and the ledger presented it as unsafe on a reading the map does
not support.

The shipped route (record the core name in the same `PinnedViews` and let the router's release
drain it) stays, for the reasons that are actually true:

1. **One uniform drain.** The core name leaves the session by the same mechanism, on the same
   paths, as the `__repark_ansi_tt_<n>` name it sits under. Two names per relation, one release
   site, one thing to keep correct — an immediate deregister would have created a second lifetime
   rule inside the same function.
2. **It releases LATER, not earlier.** The only real risk in either shape is releasing a name that
   planning still needs. Deferring to the post-planning release is strictly the safe direction
   against the "must survive planning" argument; the map's shape releases sooner.
3. **Blast radius is identical** — one function, one crate, `read_table_at` untouched, the
   reader-options caller untouched by construction. Neither shape trades that away, so this was
   never the discriminator either.

The residual recorded with the original decision is unaffected and is now stated at the fix site
too (F-8), together with a SECOND residual the panel found: `core_pinned_name` returning `None`
for a frame that did carry a pin restores the leak silently, fenced by the broadened pin.

**The lesson this is the cautionary tale for:** a deviation was flagged (good) but justified by an
invented constraint (not good). A flagged deviation still owes a true reason — flagging is not a
substitute for checking.

### 11.4 Hunk B rationale (a), narrowed (lens1 NIT-1)

§3's Hunk B cell gives two reasons for pushing the name into `pinned` **before**
`ctx.register_table(…)`:

* **(a)** "a registration that fails after taking the name is still drained" — **defensive only.**
  `register_table` here is effectively infallible: the name comes from a monotonic process-global
  counter, the preceding `deregister_table` clears any squatter, and the schema provider's only
  refusal is the duplicate-name one that step removes. Demonstrating a failure would need a
  fault-injection seam in production code, which this repo bans. It is cheap insurance against a
  future provider whose registration CAN fail, and it is correct — it is simply not something the
  pins detect, and it should never have been written as though it were.
* **(b)** "every later `?` in the loop releases what earlier turns took" — **this is the
  load-bearing half**, and it is exactly what
  `time_travel_temp_views_do_not_survive_a_failed_statement`'s mid-rewrite case exercises: the
  right-hand relation registers, the left-hand one fails to resolve, and the already-taken name
  must still drain.

**The ordering is KEPT.** Only the claim is narrowed.

### 11.5 F-1a — the collision pin, proven RED by mutation

Mutation: reintroduce a second process-global counter + local minter in
`crates/repark-spark/src/time_travel.rs` (exactly the code F-1 deleted).

**Run A — filtered (`tests::time_travel::`), where the two sequences are aligned.** The
user-facing SURVIVAL assertion reds: the reader's live registration was destroyed by an unrelated
statement.

`cargo test --locked -p repark-spark --lib tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view -- --test-threads=1`

```
running 1 test
test tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view ... FAILED

failures:

---- tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view stdout ----

thread 'tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view' (1150641) panicked at crates/repark-spark/src/tests/time_travel.rs:467:5:
assertion `left == right` failed: a time-travel STATEMENT must release every name it minted and leave the reader-options registration alone
  left: []
 right: ["__repark_tt_1"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 354 filtered out; finished in 0.07s

error: test failed, to rerun pass `-p repark-spark --lib`
```
exit `101`. `left: []` is the whole defect in one line — the reader's `__repark_tt_1` is gone.

**Run B — the FULL lib suite, where earlier tests have advanced the door's counter and the two
sequences are NOT aligned.** The survival assertion passes (the numbers happened not to collide)
and the SHARED-SEQUENCE assertion catches it instead. This is why the pin carries both halves: the
second one reds whatever the process history is.

`cargo test --locked -p repark-spark --lib -- --test-threads=1`

```
---- tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view stdout ----

thread 'tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view' (1154958) panicked at crates/repark-spark/src/tests/time_travel.rs:498:5:
the Spark door must mint from repark-core's counter, not one of its own: 2 → 3
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view

test result: FAILED. 354 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 16.23s

error: test failed, to rerun pass `-p repark-spark --lib`
```
exit `101`. `2 → 3` says it exactly: a one-relation statement ran between the two mints and
consumed NOTHING from repark-core's counter.

On the shipped tree the same filter is green (§11.9).

### 11.6 F-4 — the ANSI docstring claim, captured

Mutation: drop `pinned.release(cx.ctx)` in `crates/repark-sql/src/router.rs::execute`.

`cargo test --locked -p repark-sql --test introspection time_travel_pinned_views_do_not_leak_into_the_introspection_surface -- --test-threads=1`

```
warning: method `release` is never used
   --> crates/repark-sql/src/time_travel.rs:118:19
    |
115 | impl PinnedViews {
    | ---------------- method in this implementation
...
118 |     pub(crate) fn release(&self, ctx: &datafusion::prelude::SessionContext) {
    |                   ^^^^^^^
    |
    = note: `#[warn(dead_code)]` (part of `#[warn(unused)]`) on by default

warning: `repark-sql` (lib) generated 1 warning
    Finished `test` profile [unoptimized + debuginfo] target(s) in 13.32s
     Running tests/introspection.rs (target/debug/deps/introspection-cf26ae13694ceeb9)

running 1 test
test time_travel_pinned_views_do_not_leak_into_the_introspection_surface ... FAILED

failures:

---- time_travel_pinned_views_do_not_leak_into_the_introspection_surface stdout ----

thread 'time_travel_pinned_views_do_not_leak_into_the_introspection_surface' (1187892) panicked at crates/repark-sql/tests/introspection.rs:285:5:
time-travel temp views must be released, not left on the session: ["__repark_ansi_tt_1", "__repark_ansi_tt_2", "__repark_ansi_tt_3"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    time_travel_pinned_views_do_not_leak_into_the_introspection_surface

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 4 filtered out; finished in 0.07s

error: test failed, to rerun pass `-p repark-sql --test introspection`
```
exit `101`.

**What the capture changed.** The docstring claimed this mutation reds "both leftover assertions".
A run cannot show that — the first `assert!` panics and the second never executes. The docstring is
now narrowed to what the evidence supports: this mutation reds at the FIRST (ANSI-prefix)
assertion, naming `__repark_ansi_tt_1|2|3`; the core half leaks too but a panic reports one
assertion. The OTHER mutation named in the same docstring (drop the core-name record in
`register_pinned_view`) is the one that reds the SECOND assertion alone, and that transcript was
already captured at §7a — `["__repark_tt_1", "__repark_tt_2", "__repark_tt_3"]`, with the ANSI half
green. Between the two, each assertion is independently shown to be load-bearing.

### 11.7 Dispositions NOT implemented as written — flagged, not absorbed

1. **F-12 ("error message unchanged" → "error token contained")** — the phrase does not occur in
   this ledger or in any test comment. `grep -n "unchanged" task/h1b-ledger.md` returns one hit,
   about the `listTables` assertion, not about errors; the two pins say "must still name the
   unresolvable snapshot id" / "must still name the unknown column" and assert
   `err.to_string().contains(<token>)`, which is already the accurate formulation. The over-claim
   the finding names ("each asserting the original error message is unchanged") lives in the
   planning-side actor report §2, which the disposition places outside this fixer's scope.
   **Nothing was changed; the orchestrator needs to annotate the report.**
2. **F-16 (nine-vs-ten)** — likewise absent here: §9 lists the doc files without a count word. The
   mismatch is actor-report-internal (§1's 8.6 row says "nine map/doc files", §4's table has ten
   rows). §11.8 below records the true, post-fix-pass list so no count has to be inferred.
   **Orchestrator annotation needed on the report.**
3. **F-10 / F-11 were filed as dated corrections + pointers rather than in-place rewrites** of §4,
   §3 and §8, because this ledger is append-only for the actor's sections. The corrected reasoning
   is complete at §11.3 / §11.4 and neither original can now be read without its correction. If
   the orchestrator wants the originals struck through instead, that is a one-line follow-up.
4. **Observed, outside the disposition list, therefore NOT edited:** two further "every exit path"
   sites survive — `task/lessons.md:58` (the live DO rule "DO release ephemeral providers on every
   exit path") and `docs/history/port-v2/promotion-ledger.md:176` (the HOMED row for that same
   lesson). Both carry the wording F-2/F-6/F-9/F-13/F-18 corrected everywhere else. `lessons.md` is
   a live-rules file and the promotion row points at it, so truing them is one coupled edit — and
   it was not dispositioned. **Raised, not taken.**

### 11.8 Files this fix pass touched

**Engine + tests (8).** `crates/repark-core/src/time_travel.rs` (F-1 minter made public + doc'd,
F-14), `crates/repark-core/src/lib.rs` (F-1 re-export), `crates/repark-spark/src/time_travel.rs`
(F-1 second counter deleted, F-1b `release` doc, mint-site KEEP comment),
`crates/repark-spark/src/tests/time_travel.rs` (F-1a pin + `temp_view_sequence` helper),
`crates/repark-sql/src/router.rs` (F-2), `crates/repark-sql/src/time_travel.rs` (F-8),
`crates/repark-sql/tests/introspection.rs` (F-4), `python/repark/tests/test_time_travel.py` (F-3).

**Maps + records (14).** `crates/repark-core/src/map.md`, `crates/repark-spark/src/map.md`,
`crates/repark-spark/src/tests/map.md`, `crates/repark-spark/map.md` (F-7),
`crates/repark-sql/src/map.md` (F-13), `python/repark/src/repark/map.md`,
`python/repark/tests/map.md`, `STATUS.md` (F-5), `docs/design/v2-engine-hardening.md` (F-9),
`docs/history/port-v2/p2g-ansi-m2-ledger.md` (F-8 + F-19),
`docs/history/port-v2/port-execution-log.md` (F-6),
`docs/history/port-v2/promotion-ledger.md` (F-20), `briefs/v2-engine-hardening.md` (F-18),
`task/h1b-ledger.md` (this section + F-17 and the F-10/F-11 pointers), `task/map.md`.

Counting the ledger itself, that is **22 files** in the fix pass, on top of the 17 in the delivery.

### 11.9 The two original mutations, RE-RUN against the shipped (post-F-1) code

F-1 changed the code path both mutations exercise, so §7c and §7d no longer described the shipped
tree. Both were re-run. **Same discrimination, plus the new pin joining mutation 1.**

**Mutation 1 — drop `pinned.release(ctx)` in `execute_with_read_only`.**

`cargo test --locked -p repark-spark --lib tests::time_travel:: -- --test-threads=1`

```
warning: method `release` is never used
  --> crates/repark-spark/src/time_travel.rs:97:12
   |
76 | impl PinnedViews {
   | ---------------- method in this implementation
...
97 |     pub fn release(&self, ctx: &SessionContext) {
   |            ^^^^^^^
   |
   = note: `#[warn(dead_code)]` (part of `#[warn(unused)]`) on by default

warning: `repark-spark` (lib test) generated 1 warning
    Finished `test` profile [unoptimized + debuginfo] target(s) in 13.18s
     Running unittests src/lib.rs (target/debug/deps/repark_spark-2aa14407d468c143)

running 4 tests
test tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view ... FAILED
test tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement ... FAILED
test tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement ... FAILED
test tests::time_travel::time_travel_version_timestamp_branch_tag_and_errors ... ok

failures:

---- tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view stdout ----

thread 'tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view' (1216551) panicked at crates/repark-spark/src/tests/time_travel.rs:467:5:
assertion `left == right` failed: a time-travel STATEMENT must release every name it minted and leave the reader-options registration alone
  left: ["__repark_tt_1", "__repark_tt_2"]
 right: ["__repark_tt_1"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement stdout ----

thread 'tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement' (1216553) panicked at crates/repark-spark/src/tests/time_travel.rs:367:5:
a rewrite that failed half-way must release what it already registered: ["__repark_tt_3"]

---- tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement stdout ----

thread 'tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement' (1216554) panicked at crates/repark-spark/src/tests/time_travel.rs:328:5:
time-travel temp views must be released, not left on the session: ["__repark_tt_4", "__repark_tt_5", "__repark_tt_6", "__repark_tt_7", "__repark_tt_8"]


failures:
    tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view
    tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement
    tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement

test result: FAILED. 1 passed; 3 failed; 0 ignored; 0 measured; 351 filtered out; finished in 0.45s

error: test failed, to rerun pass `-p repark-spark --lib`
```
exit `101`. Note the **numbers changed and the shapes did not**: with one shared counter, the
reader's `__repark_tt_1` now sits ahead of the statement's `__repark_tt_2` instead of colliding
with it, and the mid-rewrite leftover is still a SINGLE name (`__repark_tt_3`) because the splice
runs right-to-left. The pre-existing twin stays green — it never asserted leftovers.

**Mutation 2 — `if result.is_ok() { pinned.release(ctx); }`.** The acceptance that matters: only
the error-path pin reds.

```
running 4 tests
test tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view ... ok
test tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement ... FAILED
test tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement ... ok
test tests::time_travel::time_travel_version_timestamp_branch_tag_and_errors ... ok

failures:

---- tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement stdout ----

thread 'tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement' (1220241) panicked at crates/repark-spark/src/tests/time_travel.rs:367:5:
a rewrite that failed half-way must release what it already registered: ["__repark_tt_6"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement

test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 351 filtered out; finished in 0.46s

error: test failed, to rerun pass `-p repark-spark --lib`
```
exit `101`. The collision pin is GREEN here, correctly: a succeeding statement still releases, so
nothing collides and nothing accumulates. It is mutation 1, not mutation 2, that it answers.

### 11.10 Restore proofs (sha256, before → after each mutation)

Every mutation was a byte copy taken immediately before it and restored from that copy, verified
after. `cargo fmt --all` ran ONCE, before any fix-pass mutation, so a single hash per file covers
the whole pass — unlike the delivery, which needed two for `crates/repark-spark/src/router.rs`.

| File | Mutations it carried | sha256 before the first, and after every restore |
|---|---|---|
| `crates/repark-spark/src/time_travel.rs` | F-1a (second counter reintroduced, runs A + B) | `13735f1e957b6806aeabc3cc572af9c82de9c16634cd7a74371ae32ac4bdcf14` |
| `crates/repark-sql/src/router.rs` | F-4 (drop `pinned.release(cx.ctx)`) | `a7225433ac6eaafcc095fb8176e084091fa307bd81079bb104ea62568e6e1408` |
| `crates/repark-spark/src/router.rs` | re-run mutation 1, re-run mutation 2 | `334624afc2ad42a33bcab71531f80fad7538a0b7f8f049a51ce025c88e35012c` |

`crates/repark-spark/src/router.rs`'s hash is **the same value the delivery's §7e restore proof
records** — the fix pass did not touch that file, and both mutations left it byte-identical.
Confirmed absent from the shipped tree afterwards: `grep -c TEMP_VIEW_SEQ
crates/repark-spark/src/time_travel.rs` → `0`; `grep -n "pinned.release"` present in both routers;
no `is_ok()` gate anywhere. No mutation identifier survives in the tree.

### 11.11 Green on the shipped tree

`cargo test --locked -p repark-spark --lib tests::time_travel:: -- --test-threads=1`

```
running 4 tests
test tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view ... ok
test tests::time_travel::time_travel_temp_views_do_not_survive_a_failed_statement ... ok
test tests::time_travel::time_travel_temp_views_do_not_survive_a_successful_statement ... ok
test tests::time_travel::time_travel_version_timestamp_branch_tag_and_errors ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 351 filtered out; finished in 0.45s
```
exit `0`.

`cargo test --locked -p repark-sql --test introspection -- --test-threads=1`

```
running 5 tests
test information_schema_enumerates_an_iceberg_catalog_through_the_ansi_door ... ok
test introspection_still_refuses_without_the_information_schema_conf ... ok
test metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_ansi_door ... ok
test show_tables_and_describe_delegate_through_the_ansi_door ... ok
test time_travel_pinned_views_do_not_leak_into_the_introspection_surface ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.18s
```
exit `0`.

### 11.12 Gates after the fix pass

| Gate | Command (real exit captured, never a pipe's) | Result |
|---|---|---|
| Full local verification | `make verify > log 2>&1; echo $?` | **`0` — GREEN, first attempt** |
| Facade against a rebuilt native module (F-1 is engine code, so this is mandatory) | `make py-test-facade > log 2>&1; echo $?` | **`0` — GREEN**, `2634 passed, 46 skipped in 93.11s` |

Counts that moved: `repark-spark` lib **354 → 355** (the F-1a collision pin); `repark-core` lib
102; `repark-sql` lib 208; `repark-sql` `tests/introspection.rs` 5. The facade count is unchanged
at 2634 — F-3 added an assertion to an existing test, not a new one.

No gate was skipped, and there was no red to record this time: `cargo fmt --all` ran once before
any mutation, which is what kept `rust-fmt-check` clean on the first `make verify` (contrast §7f).
