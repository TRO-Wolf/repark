# Card MAP-RATCHET-1: the excepted maps come under the 200-line / 16 KiB ceilings, three or four per round

**Date:** 2026-10-31. **Filed by:** Claude (claude-haiku-5-5), records clerk, on the owner's word of 2026-10-31 ("add our map md reduction path onto a near future queue").

**Status:** QUEUED near-term by the owner on 2026-10-31. Grade C clerk work (Haiku 5.5 xhigh or Muse xhigh; no frontier model writes map prose). It starts after the F-STRUCT-EMPTY-CHILD-PANIC-1, RP-63 and targeted re-audit sequence of the Iceberg label, or as fill whenever a clerk-tier lane is idle. Not started at filing.

**Why.** The map policy evaluation adopted on 2026-10-29 puts pins out of the maps first (MAP-GOVERNANCE-1, merged 2026-10-30 as PR #1221, squash `7882504c`) and the ratchet second. The ratchet is armed and holds: a tracked map may not cross 200 lines or 16,384 bytes, and an excepted map may not grow past its row. The 51 rows that were over a ceiling when the ratchet armed on 2026-10-30 are the debt that remains, and this card retires it.

**Inventory.** At main `2444083` (2026-10-30), `scripts/check_map_size.py` holds 51 `EXCEPTIONS` rows. The top 20 by bytes:

| Rank | Map | Lines | Bytes |
|---:|---|---:|---:|
| 1 | `crates/repark-spark/src/tests/map.md` | 2,812 | 237,485 |
| 2 | `crates/repark-spark/src/map.md` | 2,525 | 211,771 |
| 3 | `crates/repark-iceberg/src/write/map.md` | 2,067 | 180,962 |
| 4 | `task/roadmap/mid-term/map.md` | 1,119 | 157,044 |
| 5 | `python/repark/src/repark/spark/dataframe/map.md` | 1,892 | 143,486 |
| 6 | `crates/repark-core/src/map.md` | 1,577 | 129,326 |
| 7 | `crates/repark-functions/src/map.md` | 1,564 | 124,860 |
| 8 | `python/repark-parity/tests/map.md` | 1,141 | 123,350 |
| 9 | `crates/repark-python/src/map.md` | 301 | 107,812 |
| 10 | `python/repark/src/repark/spark/map.md` | 1,174 | 90,880 |
| 11 | `python/repark/src/repark/spark/session/map.md` | 148 | 86,774 |
| 12 | `crates/repark-core/src/session/df_guards/map.md` | 781 | 61,318 |
| 13 | `scripts/map.md` | 592 | 58,729 |
| 14 | `crates/repark-spark/tests/map.md` | 602 | 57,869 |
| 15 | `crates/repark-core/src/session/map.md` | 654 | 54,452 |
| 16 | `crates/repark-iceberg/src/catalog/map.md` | 635 | 53,841 |
| 17 | `map.md` | 304 | 43,696 |
| 18 | `crates/repark-sql/src/map.md` | 491 | 41,036 |
| 19 | `crates/repark-iceberg/src/write/merge/map.md` | 509 | 40,910 |
| 20 | `crates/repark-core/src/session/tests/map.md` | 531 | 40,886 |

The top 20 hold 21,419 lines and 2,046,487 bytes. The remaining 31 excepted maps hold 779,064 bytes together; the largest of them is `python/repark/tests/timestamps/map.md` at 221 lines and 39,702 bytes.

**Method.** The pilot's (MAP-TESTS-PILOT-1, PR #1223, squash `480ecb7b`; its history is `docs/history/facade-tests/`). Every chunk follows it:

- The map becomes a file index: a Layout table, an "I want to..." routing table by subsystem, and Pointers and Debug sections, under 200 lines and 16 KiB.
- The old map is copied verbatim to `docs/history/<dir>/<name>-map-<date>.md`, with every relative link re-based so it resolves from the new location. The pilot re-based its 443 links rather than growing the allowlist. The copy is indexed by `docs/history/<dir>/map.md` and by a row in `docs/history/map.md`.
- `scripts/docs_links_allowlist.txt` is never grown.
- The `EXCEPTIONS` row is deleted by `python3 scripts/check_map_size.py --update-baselines` once the map is within both ceilings.
- Every chunk runs `make py-test` before hand-back. Parity-harness tests read map text for navigation; the pilot's PR went red on `test_pr245_navigation_names_the_separate_revalidation_family` until a routing row was added.
- A chunk that condenses a tests map first greps `python/repark-parity/tests/` for citations of that map, and keeps a routing row for each name a test asserts.

**Chunks.** Three or four maps per PR, largest first, one clerk lane at a time. The top 20 take six PRs, using the ranks in the table above:

1. Ranks 1, 2, 3 and 5: `crates/repark-spark/src/tests/map.md`, `crates/repark-spark/src/map.md`, `crates/repark-iceberg/src/write/map.md`, `python/repark/src/repark/spark/dataframe/map.md`.
2. Ranks 6, 7, 8 and 9: `crates/repark-core/src/map.md`, `crates/repark-functions/src/map.md`, `python/repark-parity/tests/map.md`, `crates/repark-python/src/map.md`.
3. Ranks 10, 11, 12 and 13: `python/repark/src/repark/spark/map.md`, `python/repark/src/repark/spark/session/map.md`, `crates/repark-core/src/session/df_guards/map.md`, `scripts/map.md`.
4. Ranks 14, 15, 16 and 17: `crates/repark-spark/tests/map.md`, `crates/repark-core/src/session/map.md`, `crates/repark-iceberg/src/catalog/map.md`, `map.md`. The root map gets an independent verifier, because it is the entry point every reader loads first and its routes are read by `scripts/sync_map_md.py` and by the navigation pins.
5. Ranks 18, 19 and 20: `crates/repark-sql/src/map.md`, `crates/repark-iceberg/src/write/merge/map.md`, `crates/repark-core/src/session/tests/map.md`.
6. Rank 4, `task/roadmap/mid-term/map.md`, after its decision. The map shrinks by archiving closed cards through the lifecycle script, and `scripts/ledger_lifecycle.py` has no card archive today. That mechanism is a small design unit on Opus 5.5 or an owner ruling, not a hand condense.

The remaining 31 excepted maps follow in the same chunks, smallest work last.

**Measurement.** Step 6 of the owner's adopted plan measures the mandatory read path of one unit in tokens, against the 105k measured on 2026-10-29. It runs once after chunks 1 and 2 and once after the last chunk. Each run is a measurement recorded as one line, with no commit beyond that line.

**Retires.** When `EXCEPTIONS` in `scripts/check_map_size.py` is empty, and the Open bullet in `docs/maps-policy.md` records the date on which it emptied.
