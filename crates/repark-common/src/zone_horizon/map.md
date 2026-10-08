# map — repark-common/src/zone_horizon

## Purpose

Unit tests of the zone horizon ([../zone_horizon.rs](../zone_horizon.rs)).

## Contents

- `tests.rs` — **ZONE-HORIZON-RENDER-1 (2026-10-08):** the two directions of the horizon over a
  toy zone, `TabulatedToHorizon`, that copies the shape of the fault: New York's rule up to
  2099, standard time for ever after, and a marker offset before 1200. No real zone is used,
  because `repark-common` carries no zone tables; the real zones are pinned one layer up, in
  `repark-functions` (`src/tests/zone_horizon_render.rs`) and `repark-core`
  (`src/session/zone_localiser/tests.rs`). Seven tests: an instant inside the tables reads the
  zone itself; `tabulated_utc_seconds` is exactly 1200-01-01 to 2100-01-01; an instant after 2099 reads its proxy year (years 2100, 2104, 2500, 9999 and
  262000); the spring change after 2099 falls on the second Sunday of March of the real year,
  to the second; an instant before 1200 reads its proxy year; a wall clock after 2099 keeps its
  gap and its overlap; and the round trip — the wall clock of every sampled instant in
  2100–2500 (one every three days, five hours and 1861 seconds, more than 45 000 samples)
  reads back as that instant, with more than half of the samples in summer time.
  pins: zone-horizon-render-1/C-003, C-004, C-011
