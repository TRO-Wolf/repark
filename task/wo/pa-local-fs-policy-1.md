# PA-LOCAL-FS-POLICY-1 (U-5 part 2, audit PA-02 to PA-04): the local-file write policy holds below the root, through missing parents, and for every UTF-8 path     grade: B   engine: Opus 5.5 high (owner switch 2026-10-15)   release: 1.5.6

**Status:** order of record for the second of U-5's two units in the Iceberg closeout (closeout
plan row U-5; owner ruling T-5, 2026-10-13; ruling 69, 2026-10-14 night: "Both U-5 orders written
tonight, launched after triage"). Written by the orchestrator (Claude, claude-opus-5-5) on
2026-10-14 from [the production audit](../../docs/cutover/production-audit-2026-10-11.md),
findings PA-02, PA-03 and PA-04. Main at writing: `37d60eaf`. **Not launched before the
2026-10-15 triage.**

## 0. Why, and what is out of scope

With `repark.sql.allowLocalFilesystemDDL=false` (the default), RePark refuses a `COPY TO` or
`CREATE EXTERNAL TABLE` whose local destination is outside every registered warehouse. Three
executed audit findings defeat it, on both SQL doors, because the Spark door's guard
(`crates/repark-spark/src/local_fs_ddl.rs`, 373 lines) and the ANSI door's copy
(`crates/repark-sql/src/guards.rs`, 800 lines, from line 313) share the same shapes:

- **PA-02 (S1):** the guard looks only at a root `Copy` or `CreateExternalTable` plan, so
  `EXPLAIN ANALYZE COPY … TO '<outside>'` writes the file.
- **PA-03 (S1):** `canonicalize_best_effort` resolves the target or its immediate parent, then
  falls back to lexical normalization, so `<warehouse>/link/new/sub/export` with
  `link -> <outside>` and the middle directories absent passes and writes outside.
- **PA-04 (S2):** `strip_ascii_prefix_ci` (Spark, line 51) and `strip_prefix_ci` (ANSI, line 404)
  slice `value[..prefix_len]` without a character-boundary check, so a non-ASCII location panics.

Out of scope, by name: what the policy allows (the warehouse roots and the opt-in stay as they
are); remote schemes; an operating-system sandbox (the audit says the setting is not one); the
concurrent symlink-change race (see §1); plain `EXPLAIN`, which must keep answering without
writing.

## 1. Rulings already made

- **The audit's required results** for PA-02, PA-03 and PA-04 are the charter (quoted in the
  audit; read them in full).
- **Owner, 2026-10-15 (amending the order at launch, #1065):** "the public Python repark.sql()
  entry gets the guard. It is in scope, not a halt. The only halt on it is a public signature
  change outside the two guard files."; "The symlink-change race stays a dated row. One
  implementation in repark-core beside CatalogRegistry is approved; check-crate-dag proves no
  new edge."; "Both units run on Opus 5.5 high under today's switch; the records commit updates
  each order's engine line."
- **Owner, 2026-10-15 (after the verifier's FAIL, findings V-01 to V-06):** "103: (a). V-02
  comes into scope with the cause. One condition: the textual-vs-followed check is judged on the
  part of the path under the warehouse, after the warehouse root and the target are
  canonicalised the same way. Pin that a warehouse whose root is itself a symlink (macOS /tmp)
  still accepts writes inside it. Pin V-01, V-02 and V-03 each as "writes outside on today's
  head, no file on disk after". Scoped re-verifier; not green means park."; "104: (a) this
  round. The message names a real opt-in path for repark.sql() if one exists. If none exists,
  the card is filed now and the 1.5.6 note carries the exact statement as a dated loss."; "105:
  (a). Card and dated row for each now; U-5 does not grow. The CREATE TABLE ... LOCATION
  outside finding is the same class as PA-02: its card is the next U-5-kind unit on that lane
  after the re-verifier, and the 1.5.6 local-file policy line does not read clean until it
  lands. The df.write row says plainly that one write to a path makes later SQL writes to that
  path allowed."
- **Owner, 2026-10-16 (after the scoped re-verifier's FAIL, findings RV-01 to RV-03):** "114: (b).
  The guard judges where the bytes land: the followed path of the nearest existing ancestor plus
  the remaining components, against the canonical warehouse root. The four cells free. The V-01,
  V-02 and V-03 pins still pass under (b). The check-then-write race stays the dated row."; "116:
  (a) if every re-verifier finding traces to how the path is read: one scoped round on Opus xhigh,
  then the re-verifier; a second FAIL parks U-5 for good, back to a sketch, and 106 moves ahead.
  If any finding is a new bypass unrelated to the path reading, (b) now. Order U-1, 106, U-5 is
  sound under 113's wrapper condition."
- **Orchestrator, at writing (the owner may overrule at triage):**
  - **One implementation.** The two guards become one: the classification, prefix strip,
    canonicalization and containment helpers move to `crates/repark-core` beside
    `CatalogRegistry` (`crates/repark-core/src/catalog_state.rs`), which both doors already depend
    on, and both doors call them. Moved code sheds its comments. No new crate edge (check with
    `make check-crate-dag`). If the move needs any change of a public signature outside the two
    guard files, halt with the question.
  - **PA-03, threat model:** containment resolves the nearest existing ancestor through
    `canonicalize` and joins the missing tail; a destination whose nearest existing ancestor
    resolves outside every warehouse refuses. A concurrent actor who changes a symlink between
    check and write is outside this unit and is recorded as a dated row with the audit's wording;
    the owner may rule it in.
  - **PA-03, as ruled 2026-10-16 (ruling 114 (b)), replacing the clause "a tail that contains `..`
    after the nearest existing ancestor refuses":** the guard judges where the bytes land. The
    location is read as the door writes it (DataFusion's `ListingTableUrl::parse`, which removes
    every `..` lexically, `%2e` forms included); the nearest existing ancestor of that path is
    canonicalised and the remaining components are appended; the result is compared against the
    warehouse root, read as a plain path or `file:` URL with no glob split and canonicalised the
    same way. A `..` left in the remaining components after a missing directory, which no door's
    reading produces, refuses.
  - **PA-02, carriers:** the guard walks the plan and checks every `Copy` and
    `CreateExternalTable` node it finds, at any depth, before execution; `Analyze` (EXPLAIN
    ANALYZE) and an executed prepared statement are the named carriers; plain `EXPLAIN` stays a
    plan display. A walk over plan **nodes** is the fix here; it is not a list of statement
    spellings.
  - **PA-02, the `repark.sql()` callable:** the audit says the public Python `repark.sql()` builds
    `DataFusionDialect`, whose `PreExecute` belt does not call this guard. Step 0 traces it and
    reports; whether that entry gets the guard is a question for the owner (halt with a lean), not
    a choice made in the round.

## 2. Files

| Path | Change |
|---|---|
| `crates/repark-core/src/local_fs_policy.rs` | new: the shared helpers (classification, checked prefix strip, nearest-ancestor containment), with its map row |
| `crates/repark-core/src/lib.rs` (or the module that declares siblings of `catalog_state.rs`) | the module declaration |
| `crates/repark-spark/src/local_fs_ddl.rs` | calls the shared helpers; the plan walk; its private copies deleted |
| `crates/repark-sql/src/guards.rs` | the same |
| `crates/repark-spark/src/tests/` and `crates/repark-sql/tests/` (beside the existing guard tests; grep `refuses_copy_to_outside_warehouse_by_default`) | the pins |
| `python/repark/tests/test_pa_local_fs_policy_1.py` | new: the public Spark SQL pins of the audit's reproductions |
| `task/ledgers/staging/pa-local-fs-policy-1-ledger.md` | new: the unit ledger |
| `docs/cutover/production-audit-2026-10-11.md` | a dated amendment line under each of PA-02, PA-03 and PA-04 naming its pins |

Ceilings from `scripts/check_rust_file_size.py`; raising one is a halt.

## 3. Skeletons

Grade B: none.

## 4. Steps

0. **Measure, no product code.** Reproduce each audit finding on main through both doors (the
   Spark door through `repark` in Python; the ANSI door through its Rust test harness), in
   `tmp_path` directories only: PA-02 `EXPLAIN ANALYZE COPY` outside (and a prepared `COPY` if the
   door supports `PREPARE`/`EXECUTE`); PA-03 the missing-parent symlink escape with one and with
   three missing directories; PA-04 `aaaaaaé`, a non-ASCII character spanning bytes 5, 7 and 9, a
   `file://localhost` form. Paste each: written or refused, and where the file landed. Trace
   `repark.sql()` to its native entry and say whether any guard runs (§1). Commit the outputs in
   the ledger.
1. **Pins, red.** For each finding, both doors: the audit's reproduction refuses with the named
   policy error and writes no file; the warehouse control and the explicit opt-in control still
   write; plain `EXPLAIN` still answers. Red on main; paste the count. Commit.
2. **The shared helpers, no caller.** `local_fs_policy.rs` with its unit tests (the PA-04 byte
   boundaries; nearest-ancestor resolution). Commit.
3. **Both doors call them; the private copies go.** The pins of PA-03 and PA-04 go green. Commit.
4. **The plan walk (PA-02).** The pins of PA-02 go green. Commit.
5. **Records.** The ledger (one clause per sentence of each required result, each pinned), the
   three dated audit amendments, the race row if §1's threat-model line stands, map rows, one
   line per behaviour change in `task/roadmap/mid-term/v1-5-6-release-notes-draft.md`. Commit.

## 5. Gates and their expected output

| Command | Green means |
|---|---|
| `cargo test -p repark-core -p repark-spark -p repark-sql` (inside the build lock) | every test passes |
| `.venv/bin/python -m pytest python/repark/tests/test_pa_local_fs_policy_1.py -q` after a release module build | every test passes; paste the count |
| `make rust-clippy`, `make rust-panic-ban`, `cargo fmt --all -- --check` | exit 0 |
| `make check-rust-file-size check-crate-dag` | clean; no new crate edge |
| `make check-docs-links check-docs-compaction check-ledger-grammar`, `make spell-check` | exit 0 |
| `python3 scripts/sync_map_md.py --check`, `bash scripts/check_map_md.sh --base origin/main` | exit 0 |
| the comment gate | 0 hits |

CI green before the verifier: one fresh Opus 5.5 high verifier, scoped to the pins, the audit's
reproductions on both doors, and a mutant per finding (closeout plan row U-5).

## 6. Halt rules

A reproduction that does not reproduce on main (say so; that finding's pins become controls); a
need for a list of statement spellings; a public signature change outside the two guard files; a
crate edge added; a ceiling raised; a pin of an existing guard test that moves; the `repark.sql()`
question of §1; any decision this order does not make. Hand back with the question, its premise
and a lean.

## 7. Hand-back

`status`, `head`, `commits`, `step0` (every reproduction, both doors, and the `repark.sql()`
trace), `pins` (red on main, green on head, per finding and door), `moved` (each helper, from and
to), `gates`, `questions`, `out_of_scope_observed`.

Standing rules inherited from [TEMPLATE.md](TEMPLATE.md): no code comments from Anthropic models;
relocated code sheds its comments; map.md lockstep; the scratch clone, never the live worktree;
never push, never open a pull request; the TRO-Wolf identity and exactly one trailer,
`Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
