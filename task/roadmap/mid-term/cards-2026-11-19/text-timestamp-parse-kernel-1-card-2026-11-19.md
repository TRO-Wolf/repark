# Card TEXT-TIMESTAMP-PARSE-KERNEL-1: the zone-aware parse of a `TIMESTAMP` from text costs about 1.38x on a CSV read that evaluates the field; a faster kernel is the way out (cost only)

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** the Fable reply of 2026-11-19 in the agents' lessons folder (file `Fable reply — CSV schema timestamp zone unit 0 2026-11-19.md`), §1 and its option (c), kept outside the repository; the cost exception is recorded in the [unit 0 cost](../../../wo/csv-schema-timestamp-zone-1/cost.md).

**Status:** filed 2026-11-19; not dispatched. Cost only, not label-blocking. It is the way out of unit 0's named cost exception.

**Severity: S3, cost only** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's §1 option (c): "A faster kernel. Yes, as its own Rust unit, not tonight and not label-blocking.").

## Finding

- **[M]** The whole 1.38x is the text timestamp parse; `n`'s cast is about 1 ms of the 20 ms.
- **[R]** Arrow's own zoned parse does not keep Spark's rule. `arrow-cast-59.2.0/src/parse.rs:209-213` resolves a zone-less text with `from_local_datetime(..).single()`, which is an error in a DST gap and in an overlap. Spark moves a gap forward and takes the earlier offset in an overlap.
- **[R]** Fable did not read `datafusion-spark`; the registry on that box holds 55.0.0 and 55.2.0, not the pinned line.
- **[R]** RePark already owns the Spark-rule kernel (`repark-functions/src/spark_string_timestamp.rs`, `string_to_timestamp_micros`). The unit is a fast path in that kernel for the plain `yyyy-MM-dd HH:mm:ss` shape. It would speed the JSON reader and every string cast too.
- **[M]** Indicative floor: a typed scan plus a zone conversion (`ntz_cast_hour`, 63.40 ms on main) is already about 1.2x of main's `ts_only`. Main's baseline does no zone work, so 1.05x against main is probably out of reach for any correct seat.

**Main:** `k0_hour` 52.38 ms, the head 72.22 ms (1.379x); `ts_only` 52.99 ms, the head 73.33 ms (1.384x).

## Bar

**[Rec]** A kernel unit should take its bar from the indicative floor, for example at most 1.20x.

**First step:** none is given in the source. Its action, as Fable puts it: "as its own Rust unit, not tonight and not label-blocking".

Up: [map.md](map.md).
