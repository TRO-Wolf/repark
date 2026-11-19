# Card DISTRIBUTED-UDAF-INSTANCE-1: the distributed codec carries a UDAF by name only, so the executor rebuilds it from its registry default and loses the analyzer's mode and place

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** the Fable reply of 2026-11-19 in the agents' lessons folder (file `Fable reply — avg grouped run variance step 0 2026-11-19.md`), §5 "The codec card", kept outside the repository; the code reading is in §1 of the same reply. **Related:** the [avg sketch](../../../wo/avg-decimal-grouped-run-variance-1/sketch.md), §7 item 7, which records the codec as unchanged by the avg build.

**Status:** filed 2026-11-19; not dispatched. Its step 0 is pending. It does not touch the label: the label is the Spark door, and this is the distributed door.

**Severity: S2, pending its own step 0** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's §5 rating: "S2 pending its own step 0; nothing is measured yet").

## Finding

- **[R]** The reading is plausible from the code. The analyzer swaps in a UDAF instance with a different `mode` or `place` under the same name, and the executor looks the name up in its registry. `crates/repark-distributed/src/codec.rs:121-127` hands the UDAF to the inner codec unchanged, so a UDAF crosses by name.
- **[R]** The expected effect is loud, not silent: with ANSI off, the distributed door would raise where Spark answers NULL, and a `sum` could raise the other condition.
- **[R]** It is a family, not an `avg` defect. Any UDAF whose instance differs from the registered default is lost the same way. The likely seat is `try_encode_udaf` writing the mode and place into its buffer.

**Main:** not measured. The reading comes from the code, not from a distributed run.

## First step

**[Rec]** The card's step 0 first measures one ANSI-off overflow cell on the distributed door, and inventories every UDAF and UDWF that the analyzer re-instances. It matters for the product direction, not for this label.

Up: [map.md](map.md).
