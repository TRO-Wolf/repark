# task/wo/ice-tsns-type-planner-1/evidence/ — step 0 outputs

Measurement outputs of ICE-TSNS-TYPE-PLANNER-1 step 0, recorded on main
`be011012c1b6f3ae20d7bb8dadc35af472553341`. Do not edit a file by hand;
re-record it with the test or probe named beside it.

- [p5-ns-returns.txt](p5-ns-returns.txt) — the P5 walk's printed lines: every
  registered scalar, aggregate and window function return that is, or contains,
  a nanosecond timestamp over the six probe inputs. Recorded with
  `cargo test -p repark-spark --test ice_tsns_type_planner_1_p5 -- --nocapture`.
- [h2-probe.md](h2-probe.md) — the H2 probe diff (fenced), the eight spelling
  outcomes from the probe-branch release module, and the probe branch commit
  sha. The probe branch itself is never merged.
- [h3-coercion-lattice.txt](h3-coercion-lattice.txt) — the H3 walk's printed
  lines: 192 `session | op | left | right | result` cells over the four
  timestamp types on a plain session and on the Spark-door session. Recorded
  with `cargo test -p repark-spark --test ice_tsns_type_planner_1_h3 --
  --nocapture`.
- [h4-reader-types.md](h4-reader-types.md) — the H4 reader table: the Arrow
  type of a timestamp column from CSV (plain and inferred), JSON, Parquet
  (`us`, `ns`, zoned `ns`), Iceberg v2 and Iceberg v3 (`timestamp_ns`,
  `timestamptz_ns`), plus the unmeasured Postgres leg with its reason.
  Recorded with the release module of main.

Up: [../map.md](../map.md).
