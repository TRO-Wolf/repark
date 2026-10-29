# F-INTEROP-TEST-S3-DIR-1: cargo test -p iceberg leaves an s3 directory in the crate (fork card, housekeeping)

**Filed:** 2026-10-28. **Filed by:** the RP-61 clerk round (`claude-haiku-5-5`), from fork PR `#377`'s round-2 verifier (fresh Opus 5.5, PASS). Pre-existing on the fork.

**Status:** filed, not dispatched. Priority P3, housekeeping. This is the owner's fork work; no RePark lane implements it.

**Target:** the owned `iceberg-rust` fork, at the pin `56aa8913f3d1517c82e85394911b3cfc0c0d74ff`.

## The fault

`cargo test -p iceberg` leaves an untracked `s3:`-named directory inside the crate. Three interop tests resolve an s3 path as a local relative path, so the path is created on disk where the test runs.

## The fix (proposed)

The three tests resolve the s3 location through the object-store path, or write their scratch output under a temporary directory, so no directory appears in the crate.

## Pins (proposed)

A check that the crate test run leaves no untracked path under the crate directory.

## Out of scope

The interop oracle and the nightly interop workflow (RP-59's fork PRs `#370` to `#372`).

Up: [map.md](map.md).
