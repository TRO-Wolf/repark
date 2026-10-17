# Card ENC-1-RESIDUE-1: what the keyed-table refusal does not cover

**Date:** 2026-10-09. **Filed by:** Claude (Opus 5.5), ENC-1 fold 2, from the Opus re-verify of PR #1019 head `78349722`.

**Status:** reopened 2026-10-15 by the verifier's FAIL of PR #1071 (F1 to F6) and owner
rulings 96 and 97; round 2 closed the held cells under owner rulings 108 and 109. The
scoped re-verifier failed `02c36ef0` (R1 to R4); owner ruling 121 (2026-10-16) set the fix,
and the ruling-121 round (2026-10-17) closes F1 and F3 again as below.
Items 2, 3, 4 and 6 stay accepted in words. Items 1 and 5 are closed.

**Retires:** when every item below is ruled (accepted in words, or closed by a change with a pin).

## Why

The ENC-1 refusal holds: 862 keyed write attempts, 0 plaintext data, delete, manifest, puffin or stats files written into a keyed table, 0 orphans. The re-verify found five things that sit beside that claim. None writes plaintext into a keyed table through a product path. They are recorded here so they are decided, not forgotten. The ledger rows are in [enc-1-ledger.md](../../ledgers/completed/enc-1-ledger.md), section "Fold 2".

## Items

1. **A handle loaded before the key was added can stage one orphan Parquet.** The handle keeps its plain `FileIO`, so staging writes the file. The commit then reads the key from the catalog and refuses, so no snapshot is added and no pointer moves. The file stays under the table's `data/` directory, unreferenced. A fresh handle refuses at staging and writes nothing. On the SQL doors a second session that cached the table before another session added the key refuses with no file. So the orphan needs the key to arrive between a statement's table load and its staging, or a Rust caller that holds an old handle. Choice: accept the race, or sweep the staged files when the commit refuses.

2. **An unkeyed table placed under a keyed table's location writes plaintext there.** `CREATE TABLE new USING iceberg LOCATION '<keyed table directory>' AS SELECT …` runs, and so does an INSERT into a table registered from a metadata file written before the key. The files belong to the other table. The keyed table's snapshots, refs and rows do not change. Main does the same. Choice: accept (nothing is written into the keyed table), or refuse a create or register whose location lies under a keyed table.

3. **Ref-only commits publish rows that were staged before the key was added.** These run on a keyed table: the fast-forward `cherrypick_snapshot`, `set_current_snapshot`, `fast_forward`, and `CREATE OR REPLACE BRANCH main AS OF VERSION <staged>`. Each writes one metadata JSON and no data or manifest file. The rows were plaintext in the table's files before the key existed, like every other row written before the key. `publish_changes` refuses, and only by its entry check; without that check it would run like its twins. Choice: let `publish_changes` run when it is a pointer move, or name it as the one exception.

4. **Thirteen refused shapes are clean only because of their entry checks.** Plaintext safety does not depend on the entry checks: with all of them off, the catalog guard alone let no plaintext file through (700 Spark and 117 ANSI attempts). But a refused statement then leaves something behind. The five `spark.wap.branch` writes (INSERT, UPDATE, DELETE, MERGE, INSERT OVERWRITE) create the branch ref. MERGE WITH SCHEMA EVOLUTION commits the new column. `add_files` commits a property. CTAS and `writeTo().create()` with the key (two shapes) leave a `00000` metadata JSON for a table that does not exist. `publish_changes` and the three rewrite procedures with nothing to rewrite return success. With the checks on, as shipped, none of this happens. Choice: accept that unchanged state rests on the entry checks, or move each into the guard.

5. **The gate test is textual, and a catalog the caller builds is outside the guard.** `enc_1_gate.rs` matches spellings in source text. It now names the qualified `Table` spellings the verifier used, but a new alias can still pass. `Session::register_iceberg_catalog`, `CatalogRegistry::insert` and `ReparkCatalogProvider::try_new` are public and take any `Arc<dyn Catalog>`. Every product caller passes a catalog from the three builders, which is guarded. A Rust embedder that builds its own catalog gets no guard unless it calls `EncryptionGuardCatalog::install`. Choice: install the guard inside `register_iceberg_catalog`, or document the limit for embedders.

6. **A commit that moves a keyed table's metadata directory refuses on Glue.** The metadata-JSON exception is bound to the metadata directory of the loaded handle. Glue writes commit metadata through the base table's `FileIO`, so `SET LOCATION` or a change of `write.metadata.path` on a keyed Glue table refuses. Read from the fork source, not run against AWS. Choice: accept, or widen the exception to the directory the commit names.

## Interim rulings to confirm (orchestrator, 2026-10-09)

These refuse on a keyed table and are kept until the owner says otherwise: `CREATE BRANCH` on a table with no snapshot, `compute_table_stats`, `compute_partition_stats`, `rewrite_table_path`.

## Owner rulings (2026-10-14, questions 22 to 27, ruling 32; 2026-10-15, rulings 71 and 72)

1. **Closed 2026-10-15.** When the commit refuses because the table is keyed, every file
   the write staged is deleted before the refusal returns; the refusal is unchanged and
   names a file it could not delete. The shared helper is the seam with 13 call sites
   of wiring (ruling 71a). Pins: one per operation family
   (`keyed_table_refused_append_sweeps_its_staged_file`,
   `keyed_table_refused_branch_append_sweeps_its_staged_file`,
   `keyed_table_refused_write_options_doors_sweep_their_staged_files`,
   `keyed_table_refused_overwrite_sweeps_its_staged_files`,
   `keyed_table_refused_partition_overwrite_sweeps_its_staged_files`,
   `keyed_table_refused_overwrite_by_filter_sweeps_its_staged_file`,
   the two merge pins in `snapshot_commit.rs`) plus the census
   `every_stage_then_commit_site_sweeps_staged_files_on_a_keyed_refusal`, which lists
   the 13 stage-then-commit sites so a new site without the clean-up fails the suite.
2. **Accepted 2026-10-14.** An unkeyed table placed under a keyed table's location writes
   plaintext there, and that is accepted: nothing is written into the keyed table; the
   files belong to the other table. Documented with one docs line where the ENC-1
   behaviour is documented for users.
3. **Accepted 2026-10-14.** Ref-only commits stay as they are, naming `publish_changes`
   as the one exception: it refuses, and only by its entry check, while its twins run
   pointer moves.
4. **Accepted 2026-10-14.** Unchanged state resting on the entry checks is accepted:
   plaintext safety does not depend on them and the refused shapes stay clean through
   them.
5. **Closed 2026-10-15.** The public registration entry installs the guard itself, so
   every catalog a session registers is guarded, wrapping always (ruling 72a). Pins: a
   builder's catalog registered through the session (wrapped twice) refuses a keyed
   write exactly once with the same text, byte for byte, as a single wrap, and writes
   no file (`session_registration_wraps_a_builder_catalog_twice_with_one_refusal_text`);
   a directly-built catalog registered through `Session::register_iceberg_catalog`
   refuses exactly as a builder's catalog does
   (`session_registration_guards_a_directly_built_catalog`); the gate names the
   installer (`the_catalog_provider_installs_the_encryption_guard_once`, which replaced
   `session_registration_installs_the_encryption_guard` in the fix round under ruling 97).
6. **Accepted 2026-10-14.** A commit that moves a keyed table's metadata directory
   refuses on Glue, and that stays a documented refusal, with one acceptance cell to
   add when U-9's legs are built.

## Fix round (owner rulings 96 and 97, 2026-10-15)

Ruling 96, verbatim: "96: (a), as listed. One fix round on Opus 5.5 high on the same
branch, then the scoped re-verifier. Not green means park; no third round without the
delegate's look. F3 is the ruling-71 census pin, not a marker scan. F5's tests must be
able to fail."

Ruling 97, verbatim: "97: (a), installed once. The guard goes into
ReparkCatalogProvider::try_new (catalog/provider.rs:35); repark_iceberg's
register_iceberg_catalog and the Session door both build through it and inherit. No
second copy. Three door pins: a keyed INSERT refuses and leaves no unencrypted file
through each public entry. CatalogRegistry::insert keeps its entry-check pin. Same round
as 96."

- **Item 1, F1 (closed; what holds since owner ruling 121, 2026-10-17):** the clean-up
  after a refused keyed commit deletes only the files this attempt wrote: the handed files
  RePark's writers recorded and no other attempt claimed, plus the delete files a merge
  wrote in the same call. No table view chooses that set, so a file the table's current
  snapshot references (seen through a stale handle), the same file spelled as a `file://`
  URL, a file only an older snapshot or a branch references, and a concurrent writer's
  staged file all survive. The fix round's referenced-file filter is gone. The one table
  read left is ruling 115's ambiguous outcome: on a keyed table with no snapshot carrying
  the attempt's `engine.operation-id`, only the own files no snapshot references go
  (paths compared without the `file:` scheme); otherwise nothing is swept. Pins
  `enc_1_own_set.rs`, `enc_1_unknown.rs`, `enc_1_referenced.rs`, `enc_1_sweep.rs` and the
  four merge pins in `snapshot_commit.rs`.
- **Item 1, F4 (accepted as pinned, owner ruling 109):** `engine_err` keeps the refusal's
  class and the " failed to delete staged file …" suffix (pin
  `keyed_refusal_names_an_undeletable_staged_file_through_engine_err`). The micro-batch
  error keeps its class and text. The keyed sweep's warning carries the full path of the
  file it could not delete. Card MB-ENC-UNDELETED-FIELD-1 is parked under the streaming
  freeze, not a later round.
- **Item 1, F3 (closed; what holds since owner ruling 121, 2026-10-17):** the census is
  a type the compiler checks. A staged file joins an action and a transaction commits
  only inside `repark-iceberg/src/write/staged_commit.rs` (`StagedFiles`,
  `StagedCommit`); `clippy.toml` disallows `Transaction::commit` and the fork's
  file-adding action methods everywhere else, enforced by `make rust-panic-ban`. Every
  other commit goes through `write::commit_unstaged`. The 13 ruled sites and the two merge
  sites commit through the type; `repark-spark/src/ctas.rs::finish_ctas_staged_commit`,
  `repark-sql/src/create_table.rs::execute_staged_create` and
  `::publish_staged_with_summary` carry the one `#[expect]` outside the module, under
  card ENC-CTAS-REPLACE-1 (owner rulings 98 and 108). R3's five escape spellings fail
  the gate; the text-scan census and its scanner are gone.
- **Item 1, F6 (closed):** `write::append::commit_append` resolves again.
- **Item 5 (closed):** the guard is installed once, in `ReparkCatalogProvider::try_new`;
  the session's registry holds the provider's guarded handle. Door pins, registry-handle
  pin and every-door double-wrap pin in `iceberg_guard_registration.rs`;
  `CatalogRegistry::insert` entry-check pin in `enc_1_registry.rs`. F5: the item-5 pins
  measure on disk and fail with the install removed.

## Owner rulings 141 to 145 (2026-10-17)

The ruling-121 round raised four questions (142 to 145), and the owner ruled the fork bump
(141), which sets the rev the final verifier measures at. Verbatim:

```
Q141 → (a). Push now, open the fork PR, squash-merge on Tests
(default) and Tests (doc) green, as #367 was. Then the RePark
rev-bump PR at once: the five rev lines and the lock edges, nothing
else. CI, merge, acceptance run, P2 report. Then ENC-1 merges main
and its verifier measures at the new rev. That is the combination
that ships, so that is the one the verifier sees. If the merge-main
shows the ambiguous-outcome arm keyed on the old rev's error type,
re-keying it to the new one is part of the merge, not a round;
anything beyond that parks under 121. (b) is refused: it would
merge ENC-1 verified at a rev that never ships.

Q142 → (a). Accept the clippy ban through rust-panic-ban. The gap
you name gets a follow-up card at clerk grade, not in ENC-1: a
ratchet that counts allow/expect sites for the disallowed-methods
lint in lib and bin code, seeded at ENC-1's one site, so a new site
fails CI. Main has no such site in lib or bin today; the five on
main are test files the ban does not cover.

Q143 → (a). The operation id stands in until the fork exposes the
sent snapshot ids, then the switch and nothing else. Two conditions:
the id is minted per attempt, never reused across attempts or
sessions, and it is stamped in the summary before sending. Exposing
the sent ids is a fork-side card; it does not grow into DUP-1.

Q144 → (a). Keyed tables only. An unkeyed or unreadable table sweeps
nothing, as main does today. The ICE-COMMIT-UNKNOWN-1 pins stand.

Q145 → (a) for this PR. The record approach is accepted, and the
cap's direction is right: a dropped path is never swept. The
same-process edge is written into the door's map.md and named in
the P2 report, so no one reads the sweep as safe against it. The
typed-set card ranks inside the closeout, after EXPLAIN-DML-WRITE-1,
not after it.
```

- **The merge of main** (fork pin `409ff330`, #1095) needed no re-key: the ambiguous-outcome
  arm in `write/staged_commit.rs` keys on `ErrorKind::CommitStateUnknown`, which the new rev
  keeps.
- **The same-process edge** is written in `crates/repark-iceberg/src/write/map.md`, under
  `staged_commit.rs`.
- **Three cards follow:** the clerk-grade ratchet for `allow`/`expect` sites of the
  disallowed-methods lint (ruling 142), the fork-side card that exposes the sent snapshot ids
  (ruling 143), and the typed attempt set (ruling 145).
