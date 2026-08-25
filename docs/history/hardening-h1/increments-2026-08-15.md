# The 2026-08-15 increments — cut from STATUS.md on 2026-08-25 (DL-4)

Three wave records STATUS.md carried under their own headings from 2026-08-15 until the
live-document compaction. History, not law; current state is [STATUS.md](../../../STATUS.md).

## 2026-08-15 night increment (conductor-15 + Opus work group 2)

Eight more merged PRs. Engine: S-1 "spill truth" (#143 — runtime `memory_limit` now installs
a FairSpillPool so the "one truth" claim is TRUE; `temp_directory` refuses loud; RAM-relative
default `clamp(0.6 x detected, floor, 8 GiB)`; the spill regression battery landed), the M11
cardinality exemption (#140 — BL-3 retired; the last MERGE-audit divergence with a fix path
is closed), and the WI-1 store-assignment gate (#142 — INSERT OVERWRITE + append paths now
refuse un-assignable types; the four plain-INSERT doors need the WI-2 analyzer seam, named).
Debt service: column.rs -> column/ (ceiling 2200 -> 1850, #139) and udf.rs -> udf/ per-family
(2200 -> 2100, #141) — both ratcheted DOWN; the CDL Int32 prerequisite is now unblocked.
Perf: iterator-form rsi/sma, bit-exact (#138). Fork: Java battery increment 2 (#199, entries
+ readable_metrics; F-1 leaf-doc finding recorded). Recon discharged: dbt-repark (P0 session
lifetime found), fork Partitioning unification (PT-0 positional-walk corruption finding),
G6-3/G6-5 cast design.

## 2026-08-15 evening increment (conductor-14 + Opus work group)

Six more merged PRs: the five deferred window functions land (`lag`/`lead`/`nth_value`/
`percent_rank`/`cume_dist`, #133 — functions surface 291 -> 296; `column.rs` is now at its
2200 ceiling, operator-group extraction due before any further growth); the BL-4 UPDATE-path
store-assignment gate (#135) and the BL-5 abort-path cleanup (#134, with a
`CommitStateUnknown` carve-out so ambiguous commits are never corrupted) — both registry rows
retired; the M11 Spark golden RECORDED (#131, answering the audit's open question: Spark
deletes, repark refuses — the fix unit is now unblocked); and the perf baseline batteries
(#132 criterion kernels, #136 the six pipeline benches). Orchestrator profiling: TA kernels
measured at ~5% of engine time (window-exec 37% / sort 34% / arrow glue 21%) and the
unsafe-Rust ceiling measured at <=0.4% and rejected — slate priority is plan-shape work.
Also discharged by the Opus group: the spill-coverage spike (unit S-1 chartered) and the
CDL Int32-lane design.

## 2026-08-15 hardening increment (conductor-13)

Twelve PRs merged in one wave: MERGE OCC hardening (`write.merge.isolation-level` honored
(#117, audit M13), conflict batteries + M14/M15/M20 characterization pins (#121), evolved-spec
position-delete `spec_id` stamping fixed (#118, M16)); functions surface 253 -> 291 across
FN-C/D/E/F (#115/#119/#122/#125); the TA lane (fusion pins #116, `ta.with_indicators` #120,
volume goldens #123, volume kernels `ad`/`adosc`/`obv`/`mfi` #127); and the pre-authorized
stretch pair (`spark.sql.timestampType` LTZ/NTZ #124, ANSI-door nanosecond CREATE reject #126).
Remaining MERGE divergences are registered as DML-4/DML-5 and BL-3/BL-4/BL-5 in
[the divergence registry](../../spark-sql-iceberg-parity.md); TA oracle divergences stay
documented in-crate (`crates/repark-ta`), their authoritative home.
