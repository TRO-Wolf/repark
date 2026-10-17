# map — repark-iceberg/tests

## Purpose

Integration tests that drive `repark-iceberg` through its public API only, each as its own
test binary. A test lives here, not in the in-crate `src/**/tests` modules, when it needs a
process-wide setting that would race the crate's other unit tests in one binary.

## Contents

- `enc_1_sweep_warning.rs` — **ENC-1-RESIDUE-1, owner rulings 109 and 121 (2026-10-17):**
  the ruling-109 warning pin, moved here from the in-crate `write/encryption.rs` tests
  because it missed its event 8 times in 60 there (re-verifier finding R5). This binary
  holds one test and installs the only subscriber with `set_global_default`, so no sibling
  test can leave the warning's call site cached as disabled. A keyed refusal through
  `write::commit_append`, a staged file in a directory locked read-only: the session error
  names the file, and the captured `WARN` carries `message` "failed to delete a staged file
  after a refused commit" and `path` equal to the staged `DataFile` path in full.

## Pointers

- Up: [../map.md](../map.md)
- The sweep: [../src/write/staged_commit.rs](../src/write/staged_commit.rs)
