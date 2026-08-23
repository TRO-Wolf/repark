# Unit ledger — Y-4 / G4b-R1: declared rename (ships alone)

**Unit:** Y-4 (G4b-R1) of overnight conductor-4 · **Date:** 2026-08-12 · **Lane:** repark ·
**Executor:** Grok · **Worktree:** `/tmp/grok-y4` · **Branch:** `grok/y4-g4br1-rename` ·
**Base (FROZEN, A11):** `a985edf7e22b68ea720cb2a8e08fca6cdd1a33b7` · **Path:** LIGHT ·
**critic:** identity, not design.

**Charter:** workspace brief `BRIEF-y4-g4br1-rename.md` + conductor
`BRIEF-overnight-conductor-4.md` Addendum 2026-08-12 (A3 binds the TZ-5 target name).
docs/testing.md "Relocation discipline" §2: a pin's name is part of the pin; the rename ships
**alone** with an explicit old→new map.

This unit is rename-ONLY. Zero behavior edits, zero new assertions, zero reworded notes beyond
the name strings. The registry, `_live_parity.py`, and live size pins are never-touch; citation
updates live in §6 as paste-true text.

---

## 1. Rename map executed (BOUND — do not invent names)

| # | Old name | New name | Source of the target |
|---|---|---|---|
| 1 | `df_left_semi_unsupported` | `df_left_semi_on_name` | `task/g4b-join-widening-ledger.md` §6 item 1 |
| 2 | `df_left_anti_unsupported` | `df_left_anti_on_name` | `task/g4b-join-widening-ledger.md` §6 item 1 |
| 3 | `timestamp_to_int_spark_seconds_repark_raises` | `timestamp_to_int_nullability` | conductor-4 A3 (corpus-family name) |

Live-mirror token `cast_timestamp_to_int_nullability` is **UNCHANGED**.

Pytest node ids (the identity map; apply this and the collect-only diff is empty):

```
python/repark/tests/test_join_parity.py::test_join_parity_row[df_left_semi_unsupported]
  → python/repark/tests/test_join_parity.py::test_join_parity_row[df_left_semi_on_name]

python/repark/tests/test_join_parity.py::test_join_parity_row[df_left_anti_unsupported]
  → python/repark/tests/test_join_parity.py::test_join_parity_row[df_left_anti_on_name]

python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[timestamp_to_int_spark_seconds_repark_raises]
  → python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[timestamp_to_int_nullability]
```

No other node id moved. Counts are unchanged (join corpus still 30 `ROWS` + 1 probe row;
cast-failure corpus still 10 `ROWS` + 1 synthetic exemplar).

---

## 2. Live name-gates updated (and nothing else)

Y-4 is tonight's only writer of the two corpus modules.

| Site | What changed |
|---|---|
| `python/repark/tests/test_join_parity.py` | `name=` on the two G4b flip rows; the name-gated `for flipped_name in (...)` budget pin; docstring mentions of the two names (identity substitution) |
| `python/repark/tests/test_cast_failure_parity.py` | `name=` on the TZ-5 flip row; the two classifier tests that select the row by name (`test_content_disclosure_classifier_converged_arm`, `test_content_disclosure_classifier_regression_arm`) |
| `python/repark/tests/map.md` | G4b / G6 bullets that spelled the old names as current pins; ledger pointers |
| `task/map.md` | this ledger, lockstep |

Not edited (by design):

- `docs/spark-sql-iceberg-parity.md` (registry — orchestrator; §6)
- `python/repark/tests/_live_parity.py` and `test_parity_live.py` live size / disclosure-name
  pins (live-mirror token already matches A3; the corpus citation still uses the old
  timestamp name — §6)
- other units' historical ledgers (`g4b`, `w3`, `x1`, `tz5`, `l1`) — old node ids stay as
  history
- never-touch set: `CLAUDE.md` / `AGENTS.md` / `PROJECT.md` / `STATUS.md` / `briefs/` /
  `.github/` / `Cargo.lock` / `uv.lock` / `planning/hardening/*`

---

## 3. Identity gate

Source-level `name=` extraction at the frozen base vs tip, after applying the declared map:

| Module | `name=` count | Multiset after map |
|---|---|---|
| `test_join_parity.py` | 31 (30 `ROWS` + `_classifier_probe_split`) | identical except the two mapped ids |
| `test_cast_failure_parity.py` | 11 (10 `ROWS` + `synthetic_repark_raises_split_exemplar`) | identical except the one mapped id |

The two classifier tests in `test_cast_failure_parity.py` are not parametrized on the old
name (they look the row up); their own pytest node ids are unchanged.

`pytest --collect-only -q` at tip (native module present) lists the three new ids and
zero old ids; 50 tests collected across the two modules.

---

## 4. Findings (left as-is)

Rename-ONLY forbids rewording notes beyond the name strings. After the substitution:

1. `test_join_parity.py` module docstring and the two flip-row `note=` strings still say
   "names kept byte-identical" / "the `_unsupported` suffix retires in a declared-rename
   unit". That is G4b-era rationale, now historical. Not rewritten.
2. `test_cast_failure_parity.py` row `note=` still says "the rename ships alone per
   relocation discipline". Same class. Not rewritten.
3. No vacuous name-gated pin and no duplicate was exposed. The G4b `flipped_name` loop
   still binds the two name-key content rows; the G6 classifiers still bind the one
   nullability disclosure.

---

## 5. Gate evidence

Real exit codes, never a pipe's. Logs under `/tmp/y4-*.log`.

| Gate | Command | Exit |
|---|---|---|
| `make verify` | `make verify > /tmp/y4-verify.log 2>&1; echo $?` | **0** |
| `make preflight` | `make preflight > /tmp/y4-preflight.log 2>&1; echo $?` | **0** (`2822 passed, 71 skipped` facade) |

---

## 6. Handoff — paste-true citation updates (orchestrator owns the files)

> Do **not** paste from this unit. The blocks below are the exact old → new citation
> replacements for the never-touch files. Live-mirror token
> `cast_timestamp_to_int_nullability` is already correct and must stay.

### 6.1 `docs/spark-sql-iceberg-parity.md` — G6-4 pin line

**Find** (G6-4, current `main` @ `a985edf`):

```
- **Pin** — `python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[timestamp_to_int_spark_seconds_repark_raises]`
  (name kept byte-identical after the #64 flip; rename queued per relocation discipline).
```

**Replace with:**

```
- **Pin** — `python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[timestamp_to_int_nullability]`
```

The `live-mirror: cast_timestamp_to_int_nullability` line immediately below is **unchanged**.

### 6.2 `docs/spark-sql-iceberg-parity.md` — REG-G4-1 / REG-G4-2 FIXED note

**Find:**

```
> `test_join_parity.py::test_join_parity_row[df_left_semi_unsupported]` and
> `…[df_left_anti_unsupported]` are now content equalities (names kept; rename queued). A
> fixed defect gets this dated note, never a live divergence row.
```

**Replace with:**

```
> `test_join_parity.py::test_join_parity_row[df_left_semi_on_name]` and
> `…[df_left_anti_on_name]` are now content equalities. A
> fixed defect gets this dated note, never a live divergence row.
```

### 6.3 `python/repark/tests/_live_parity.py` — Disclosure note corpus citation

The disclosure **name** is already `cast_timestamp_to_int_nullability` (do not retitle).
Only the corpus node-id in the note is stale.

**Find** (inside the `cast_timestamp_to_int_nullability` `Disclosure`):

```
        "test_cast_failure_parity.py::test_cast_failure_row"
        "[timestamp_to_int_spark_seconds_repark_raises].",
```

**Replace with:**

```
        "test_cast_failure_parity.py::test_cast_failure_row"
        "[timestamp_to_int_nullability].",
```

No other `_live_parity.py` or `test_parity_live.py` token changes.

---

## 7. Authorship

Commits authored **TRO-Wolf** (`64240326+TRO-Wolf@users.noreply.github.com`) with the
`Authored-By: Grok (grok-4.5) <noreply@x.ai>` trailer, per-command `-c` identity only.
No co-author trailers, no session ids or URLs. No push, no PR (conductor).
