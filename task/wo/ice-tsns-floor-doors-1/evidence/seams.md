# ICE-TSNS-FLOOR-DOORS-1 step 0: seam list

Method: code reading on this head plus EXPLAIN of each door's statement as the
cross-check. No product code changed, no throwaway branch built, no marker
function or plan walk added. All file and line citations are on this head.
Entry points: `crates/repark-functions/src/timestamp_ns_cast.rs` lines 55
(`timestamp_ns_cast_expr`), 415 (`narrow_timestamp_ns_expr`) and 510
(`conform_values_timestamp_columns`); `crates/repark-iceberg/src/write/` and
`crates/repark-spark/src/` for the doors.

## The narrowing expressions

N1. The RePark kernel `__repark_narrow_timestamp_ns__`, emitted by
`narrow_timestamp_ns_expr` (`timestamp_ns_cast.rs:415`) and executed by
`narrow` (`timestamp_ns_cast.rs:478`). Nanosecond ticks narrow by
`div_euclid` (floor) at line 488; a zoneless (naive-wall) source is then
localised into the session zone by `localize_wall_micros_in_zone` (lines
498-506). A zoned (instant) source skips localisation (line 495). This is the
INSERT behaviour measured in D1: floor plus the session-zone wall.

N2. The Arrow cast kernel `cast_with_options` with `safe: false`
(`write/conform.rs:174` in `conform_batch_scoped`, `write_schema` batch
conform; `write/store_cast.rs:270` in `cast_store_array`). It narrows batch
columns to the write schema with no zone step: a naive wall keeps its tick
count divided toward zero (the UTC reading), so pre-epoch sub-microsecond
ticks truncate and non-UTC walls read as UTC.

N3. A DataFusion `Expr::Cast` inserted by store-assignment SQL:
`write/update_cast.rs:12` (`store_assignment_cast_sql`), used by the MERGE
insert wrapper `write/merge/insert.rs:143-150` (`wall_cast_sql` for wall
targets, otherwise the store-assignment cast for instant targets). A cast node
on a nanosecond source into a microsecond target evaluates through
DataFusion/Arrow cast semantics (toward zero, no session-zone localisation).

The analyzer rule that decides between N1 and a cast node for plan-side SQL is
`functions/instant_ts.rs:607-617` (`rewrite_cast`): a nanosecond source cast
into a nanosecond-naive or LTZ target is rewritten to N1. `rewrite_plan`
(`instant_ts.rs:553`) also runs `conform_values_timestamp_columns`
(`timestamp_ns_cast.rs:510`) over VALUES lists.

## Per-door chain (LTZ microsecond target, nanosecond source)

INSERT. Plan-side N1 through `instant_ts.rs:614`. The projection carries
`__repark_narrow_timestamp_ns__(ns)` into `IcebergWriteExec`, and the write
schema already matches, so no second narrowing fires. Measured: floor plus
session zone. EXPLAIN cross-check pasted below.

INSERT OVERWRITE. EXPLAIN shows the same N1 projection as INSERT (pasted
below), but the stored values truncate with UTC walls. The executed narrowing
is therefore not the displayed N1 alone: the overwrite commit path re-narrows
through N2/N3 after the projection. Exact winning site between
`overwrite.rs`, `partition_overwrite.rs` and the store-assignment cast is not
pinned in step 0; the candidates are `conform.rs:174` and
`update_cast.rs:12`.

MERGE insert arm. Wrapper SQL in `merge/insert.rs:105-158`
(`zone_wrapping_stream_sql`): for a microsecond LTZ target
`wall_cast_udf_name` (`ntz_store.rs:33`) returns no wall kernel, so the arm
takes the `instant` branch (`merge/insert.rs:145-150`) into
`store_assignment_cast_sql` (N3). Measured: truncate with UTC walls, matching
N3 rather than N1. (EXPLAIN of MERGE is refused by the facade router, so the
cross-check here is code plus measured values, not a plan paste.)

MERGE update arm. Measured: floor plus session zone on LTZ, identical to
INSERT. Chain: plan-side N1 (same `instant_ts.rs:614` rewrite of the
`SET v = s.ns` cast). The precise update-arm emit site in
`write/merge/` is not pinned line-exact in step 0.

UPDATE from a sibling column (v3). Measured: floor plus session zone on LTZ;
v2 refuses before narrowing (`Invalid type for c: ... not supported until
v3`). Chain: plan-side N1 for the `SET v = c` assignment; candidate emit
through `update_cast.rs:12` resolving to N1 for this shape (line-exact choice
not pinned in step 0).

`writeTo().append()` and `insertInto` append. Measured: floor plus session
zone on LTZ. Chain: the DataFrame logical plan passes the same analyzer
rewrite (N1) before the append write (`write/append.rs:43` conforms already
microsecond batches, a no-op).

`overwritePartitions` and `insertInto(overwrite=True)`. Measured: truncate
with UTC walls on LTZ. Chain: the overwrite write path narrows the
nanosecond batches through N2 (`conform.rs:174`) with no zone step,
overriding or bypassing the plan-side N1.

## NTZ microsecond target (both sources, every door)

Measured: every door truncates, INSERT included; the only deviations are the
Asia/Kolkata `timestamptz_ns` pre-epoch trio sitting 1 micro above the floor
(truncation before the zone shift). N1 never decides these cells: the path is
a wall kernel or Arrow cast that divides toward zero without flooring.
Candidate sites are `ntz_store.rs:33-55` (`wall_cast_udf_name`,
`store_kernel_sql`) with `store_cast.rs:270` beneath; line-exact choice not
pinned in step 0.

## Wall localisation

Session-zone localisation happens only inside N1
(`timestamp_ns_cast.rs:498-506`). N2 and N3 localise nowhere: walls keep
their tick counts (read as UTC) and instants convert without the session
zone, which is why the same tick stores two values depending on the door.

## EXPLAIN cross-check (release module, America/New_York)

INSERT INTO ice.ns.t SELECT id, ns AS v, 0 AS k FROM ice.ns.src:

    Dml: op=[Insert Into] table=[ice.ns.t]
      Projection: ice.ns.src.id AS id, __repark_narrow_timestamp_ns__(ice.ns.src.ns) AS v, Int32(0) AS k
        TableScan: ice.ns.src projection=[id, ns]
    IcebergCommitExec: table=ns.t
      IcebergWriteExec: table=ns.t
        UuidTextToBytesExec
          ParallelProjectionExec: expr=[id@0 as id, __repark_narrow_timestamp_ns__(ns@1) as v, 0 as k]
            CooperativeExec
              IcebergTableScan projection:[id,ns] predicate:[] N=1

INSERT OVERWRITE ice.ns.t SELECT id, ns AS v, 0 AS k FROM ice.ns.src:

    Dml: op=[Insert Overwrite] table=[ice.ns.t]
      Projection: ice.ns.src.id AS id, __repark_narrow_timestamp_ns__(ice.ns.src.ns) AS v, Int32(0) AS k
        TableScan: ice.ns.src projection=[id, ns]
    IcebergCommitExec: table=ns.t
      IcebergWriteExec: table=ns.t
        UuidTextToBytesExec
          ParallelProjectionExec: expr=[id@0 as id, __repark_narrow_timestamp_ns__(ns@1) as v, 0 as k]
            CooperativeExec
              IcebergTableScan projection:[id,ns] predicate:[] N=1

The two plans display the same N1 expression while storing different values,
so the overwrite seam lies downstream of the displayed projection. MERGE
EXPLAIN is refused by the facade router (`Unsupported SQL statement`), and
DataFrame writers have no SQL EXPLAIN; those doors rest on code plus measured
values.

There are three distinct narrowing points: the RePark floor-plus-zone kernel, the write-side Arrow cast, and the store-assignment DataFusion cast.
