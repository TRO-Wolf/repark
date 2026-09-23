# map — repark-core/src/iceberg_path

## Purpose

Tests for `../iceberg_path.rs` — the `format("iceberg").load(<path>)` static-table read
(U10 / R-DF-LOAD-PATH + R-DF-LOAD-METADATA-JSON, 2026-09-23).

## Contents

- `tests.rs` — the metadata-location picker battery: `NNNNN-<uuid>.metadata.json` selects the
  highest numeric prefix (00010- beats 00002-), `v<N>.metadata.json` selects the higher N
  (v10 beats v9), names outside the two forms are ignored, `version-hint.text` wins over a
  directory listing, a trailing-slash location resolves through the listing, and a missing
  location raises an `Error::Analysis` naming the supplied path.
  pins: dfload-1/C-003, C-005
