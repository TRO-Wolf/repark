# RP-62-EQDEL-REPIN-1: repin the fork to the equality-delete dropped-key fix, with a facade pin

**Filed:** 2026-10-28. **Filed by:** the RP-61 clerk round (`claude-haiku-5-5`), from fork PR `#377`'s round-2 verifier (fresh Opus 5.5, PASS) and the landed line of the fork card.

**Status:** merged 2026-10-29 as `df43dca5` (PR #1213, 25 checks green); verification round 1 PASS; closed. Clerk grade. Depended on RP-61 (PR #1209), whose pin history and lock are its base.

**Target pin:** `935cc55c`, the squash of fork PR `#377` (F-EQDEL-DROPPED-KEY-1 fold 2: equality deletes resolve dropped, nested and re-added key columns by field id in every format; AUD-004 on the fork). The fork card is [f-eqdel-dropped-key-1-card-2026-10-28.md](f-eqdel-dropped-key-1-card-2026-10-28.md).

**Why:** RP-61 stays pinned to `56aa8913`, the verified state its pins were built on, and `935cc55c` merged after that pin was chosen. At the current pin a scan of a v2 table whose equality-delete key column was later dropped fails with `field not found`; the registry row F-EQDEL-DROPPED-KEY-1 has no pin yet.

## Steps

0. **Measure first.** The audit ran the dropped-key sequence through the fork's Rust APIs only. Run the same schema change through the RePark SQL route at the current pin, and record the result, before the repin.
1. **The repin.** Move the five `rev` lines of the root `Cargo.toml` to `935cc55c` through `make bump-fork-pin REV=<sha>` (fork-sync rule 1). `Cargo.lock` moves only its six fork source lines. Check with `cargo metadata --locked`. Never use `cargo update -p ... --precise`, which re-points duplicate edges.
2. **A Rust pin of the AUD-004 shape through both doors, in place of the facade pin.** A v2 table `{id, data}` with five rows, an equality delete of ids 20 and 40, the key column dropped, then a read of `data`: the three survivors `a`, `c` and `e`, in the same session, a fresh Spark session and a fresh native session. Red first on the current pin. Re-worded at build (2026-10-29): no RePark statement on either door and no facade call writes an equality delete (the fork's DataFusion provider writes position deletes only), so the facade pin cannot be written. The pin is `crates/repark-sql/tests/eqdel_dropped_key_door.rs`, which reaches both doors through the `repark-spark` dev edge, because the Spark-door crate has no declared edge to `AnsiDialect`. The Avro and ORC data cases are pinned loud-or-correct, not as survivors (fork card F-EQDEL-DROPPED-KEY-2).
3. **The registry row.** Re-date F-EQDEL-DROPPED-KEY-1 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) with the repin and the step-2 pin.
4. **The records.** The fork-sync pin-history row and the root map line, and the unit ledger, as RP-61 did.

## Gates

The repin PR's CI, plus `make rust-panic-ban`.

## Out of scope

Position-delete formats (F-POSDEL-FORMAT-READ-1, pinned by RP-61). The promoted-key `_pos` projection defect (fork card F-EQDEL-PROMOTED-KEY-POS-1).

Up: [map.md](map.md).
