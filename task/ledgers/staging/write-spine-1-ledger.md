# Unit ledger — WRITE-SPINE-1 steps (1) and (3) · shared kernel names, a fail-closed lookup, the door matrix enumerated from the code

**Date:** 2026-10-24 · **Branch:** `feat/write-spine-1-steps-1-3a` · **Base:** `7a789fc` ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`), lane A · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: release 1.5.6.

**Order:** [../../wo/write-spine-1/order-steps-1-3.md](../../wo/write-spine-1/order-steps-1-3.md);
card [WRITE-SPINE-1](../../roadmap/mid-term/write-spine-1-card-2026-10-22.md); door table
[crate-boundary-eval-2026-10-22.md](../../wo/write-spine-1/crate-boundary-eval-2026-10-22.md) §3.
**This ledger closes when the pull request of steps (1) and (3) merges.**

## Step 1a, the names (2026-10-24)

`crates/repark-common/src/udf_names.rs` holds seven `pub const` names. `repark-functions`
(`timestamp_ntz_cast.rs`, `timestamp_ns_cast.rs`, `cast_map.rs`, `zone_seam.rs`) and
`repark-iceberg` (`write/ntz_store.rs`, `write/store_cast.rs`, `write/struct_pairing.rs`)
re-export them under their existing identifiers. `cargo build --workspace`, `make rust-clippy`
and `make rust-panic-ban` exit 0. The evidence grep for C-001, on the step 1b head:

```
$ grep -rn -E '"__repark_(cast_timestamp_ntz|cast_timestamp_ns|cast_timestamptz_ns|cast_map|zone_seam|store_int_guard|store_pair)__"' crates/
crates/repark-common/src/udf_names.rs:1:pub const TIMESTAMP_NTZ_CAST_NAME: &str = "__repark_cast_timestamp_ntz__";
crates/repark-common/src/udf_names.rs:2:pub const TIMESTAMP_NS_CAST_NAME: &str = "__repark_cast_timestamp_ns__";
crates/repark-common/src/udf_names.rs:3:pub const TIMESTAMPTZ_NS_CAST_NAME: &str = "__repark_cast_timestamptz_ns__";
crates/repark-common/src/udf_names.rs:4:pub const CAST_MAP_NAME: &str = "__repark_cast_map__";
crates/repark-common/src/udf_names.rs:5:pub const ZONE_SEAM_NAME: &str = "__repark_zone_seam__";
crates/repark-common/src/udf_names.rs:6:pub const STORE_INT_GUARD_NAME: &str = "__repark_store_int_guard__";
crates/repark-common/src/udf_names.rs:7:pub const STORE_PAIR_KERNEL_NAME: &str = "__repark_store_pair__";
```

On the base the same grep finds ten definitions: two copies each of the three timestamp names
(`ntz_store.rs:15-17` and `timestamp_ntz_cast.rs:20`, `timestamp_ns_cast.rs:33-34`) and one each
of the other four. Test SQL text that names a kernel inside a longer string is not a definition
and is unchanged.

## Step 1b, fail closed (2026-10-24)

`zone_store_frame` in `crates/repark-iceberg/src/write/ntz_store.rs` looks the store kernel up
through `registered_store_kernel`. When `wall_kernel_reads` holds for the target and
`store_kernel_udf_name` names a kernel the session does not hold, the lookup returns
`DataFusionError::Internal` with the kernel name, and the expression map collects into
`Result<Vec<Expr>>`. A target that needs no kernel still answers `Ok(None)`, so every other arm
is unchanged.

**Correction of the order's wording, measured (2026-10-24).** The order's §2 describes the test
as "an `INSERT … BY NAME` of a wall into a `TIMESTAMP` column". That shape never reaches the
lookup: `wall_kernel_reads(Timestamp(µs, None), Timestamp(µs, Some(_)))` is false, and the
statement takes the plain-cast arm. Measured on the base code in New York, with and without
`__repark_cast_timestamp_ntz__` registered, `INSERT INTO t BY NAME SELECT id, w AS ts` of the
wall `2026-01-02 03:04:05` into a `TIMESTAMP` column stores `1767341045000000` both times (the
session instant, Spark's answer). The shape that reaches the lookup the order's mechanism names is
the reverse: an instant into a `TIMESTAMP_NTZ` column. Measured on the base code in New York:

| statement | kernel registered | stored |
|---|---|---|
| `INSERT INTO t BY NAME SELECT id, x AS ts FROM src` (`x TIMESTAMP` → `ts TIMESTAMP_NTZ`) | yes | `1767305045000000` (the New York wall) |
| the same | no | `1767323045000000` (the UTC wall) |
| `INSERT INTO t SELECT id, x FROM src` (positional) | no | `1767305045000000` (the analyzer path, no lookup) |

The pin therefore writes an instant into a `TIMESTAMP_NTZ` column BY NAME. The orchestrator
confirms or rules otherwise.

The red-then-green record, `cargo test -p repark-spark --test store_kernel_registry`:

```
head (ntz_store.rs changed):
test missing_wall_kernel_refuses_instead_of_storing_utc ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
base lookup (ntz_store.rs stashed, the test kept):
test missing_wall_kernel_refuses_instead_of_storing_utc ... FAILED
thread 'missing_wall_kernel_refuses_instead_of_storing_utc' panicked at crates/repark-spark/tests/store_kernel_registry.rs:99:10:
a missing wall kernel refuses the write: ()
test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
```

## PROPOSITION LEDGER — WRITE-SPINE-1 steps (1) and (3) — 2026-10-24

| id | claim | evidence | status | proof |
|---|---|---|---|---|
| C-001 | Every UDF name both `repark-functions` and `repark-iceberg` name is one constant in `repark_common::udf_names`, and no call site changed. | `grep -rn '"__repark_' crates/` shows each of the seven strings once, in `udf_names.rs`. | **PROVEN** | the grep above; the step 1a diff changes only the seven definitions into re-exports; `cargo build --workspace`, `make rust-clippy` green. |
| C-002 | A session whose NTZ wall kernel is not registered refuses an `INSERT … BY NAME` of an instant into a `TIMESTAMP_NTZ` column, where main stores the UTC wall (corrected 2026-10-24 from the order's "a wall into a `TIMESTAMP` column", measured above). | `store_kernel_registry.rs`, red on main, green on head. | **PROVEN** | `missing_wall_kernel_refuses_instead_of_storing_utc`: red with the base lookup, green on head; the same test's registered control stores `1767305045123456`. |
| C-003 | `WriteDoor::ALL` holds exactly the eighteen value-writing doors of the evaluation's table, and `door_of` is an exhaustive match over the dispatched statement shapes. | `write_door.rs`; the enumeration tests; a removed variant reds them. | OPEN | step 3a. |
| C-004 | Every door has at least one spelling and every spelling names a door. | `every_write_door_has_a_spelling`, `every_spelling_names_a_door`. | OPEN | step 3a. |
| C-005 | The door matrix on this head equals main's recorded answers on every cell, and classifies against the Spark fixture as the step-0 census table says. | `test_write_spine_1_doors.py` against the two fixtures. | OPEN | step 3b. |
| C-006 | The four mutants of step 5 each red a named test. | the ledger's mutant table. | OPEN | step 5. |
