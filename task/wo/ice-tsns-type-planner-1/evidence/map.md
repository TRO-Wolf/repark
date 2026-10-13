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

Up: [../map.md](../map.md).
