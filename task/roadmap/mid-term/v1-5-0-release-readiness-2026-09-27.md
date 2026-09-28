# v1.5.0 release readiness — audit the record (2026-09-27)

**Filed:** 2026-09-27, WO RELEASE-READY-1, guided round, docs only, on branch
`docs/release-readiness-2026-09-27`, base `origin/main` `707e8a52`. **Scope:** the owner rules
the tag, the version bump and the release PR; this report audits the record and drafts nothing
but words. **Inputs:** the compare-only matrix of 2026-09-27 02:25 on main `9aa1c185`
(`/tmp/oc-worker/scoreboard/2026-09-26/matrix.json`, `compare.txt`, read-only) plus the C-5
harness rule: **705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT, 842 cells**
— the evening rerun of 2026-09-27 (fresh build on main `f7422565`) re-measures the same
verdicts; see the closing note below. **The gate:** zero non-EQUAL cells
that Spark answers, except the dated carve-outs C-1 to C-5.

The owner ruled on finding F-1 on 2026-09-27: U12 returns to v1.5.1. This file closes when the evening rerun lands.

**Closed 2026-09-27:** the evening rerun landed with the same matrix (705 EQUAL /
132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT on `f7422565`), and v1.5.0
shipped as tag `v1.5.0` (release PR #871, squash `9392dbc3`; PyPI `repark 1.5.0`).
This file is now a record; the shipped notes are the
[final v1.5.0 release notes](v1-5-0-release-notes-draft-2026-09-27.md).

## 1. Matrix against the record

Every cell in `matrix.json` whose verdict is not EQUAL and not SPARK-CANNOT. There are five,
all REFUSED-REGISTERED; DIFFERENT is zero. No cell lacks a record, so this section raises no
numbered finding; the two thin records (C-1, C-2 citations) are findings F-4 and F-5 in §6,
fixed in this round.

| Cell | Verdict (`reg_hint`) | Cover | Spec record | Parity-doc record |
|---|---|---|---|---|
| `R-STREAM-READ` | REFUSED-REGISTERED (`SES-DECL-readStream`) | Carve-out C-1 (owner, 2026-09-19) | [v1-5-0-remainder-spec-2026-09-23.md](v1-5-0-remainder-spec-2026-09-23.md) "Carved out" ¶1 (lines 221–222) | [§SES-DECL-readStream](../../../docs/spark-sql-iceberg-parity.md) (line 3271); C-1 citation added 2026-09-27 (F-4) |
| `R-STREAM-READ-SKIP` | REFUSED-REGISTERED (`SES-DECL-readStream`) | Carve-out C-1 (owner, 2026-09-19) | same spec ¶1 (lines 221–222) | same row (line 3271); C-1 citation added 2026-09-27 (F-4) |
| `W-STREAM-WRITE-FILESRC` | REFUSED-REGISTERED (`SES-DECL-streams`) | Carve-out C-1 (owner, 2026-09-19) | same spec ¶1 (lines 221–222) | [§SES-DECL-streams](../../../docs/spark-sql-iceberg-parity.md) (line 3286); C-1 citation added 2026-09-27 (F-4) |
| `D-NS-NESTED` | REFUSED-REGISTERED (`NS-2`) | Carve-out C-2 (owner, 2026-09-24) | spec U5 owner item (lines 133–135) and "Carved out" ¶2 (lines 224–225) | [§NS-2](../../../docs/spark-sql-iceberg-parity.md) (line 1478); C-2 citation added 2026-09-27 (F-5) |
| `TY-VARIANT-V3` | REFUSED-REGISTERED (`V3-VARIANT-SHRED-1`) | Carve-out C-4 (owner, 2026-09-27) | spec U9 (line 175) and "Carved out" ¶4 (lines 235–243); decision 6 closed (line 269) | [§V3-VARIANT-SHRED-1](../../../docs/spark-sql-iceberg-parity.md) (line 2769), residue line 2796 → [ice-variant-1-6.md](ice-variant-1-6.md) |

Two further cells count as EQUAL only under a dated harness rule (both recorded, no finding):

| Cell | Verdict | Cover | Spec record | Parity-doc record |
|---|---|---|---|---|
| `P-RDF-PARTIAL-PROGRESS` | EQUAL under `commit_order` | Carve-out C-3 (owner, 2026-09-26) | spec "Carved out" ¶3 (lines 227–233) | [§ICE-RDF-OPTIONS-1](../../../docs/spark-sql-iceberg-parity.md) (line 9114) |
| `L-INSERT-OVERWRITE` | EQUAL under `row_id_unordered` | Carve-out C-5 (owner, 2026-09-27) | spec U0 row (line 65) and "Carved out" ¶5 (lines 245–254) | row-lineage residue (line 10254) |

## 2. Carve-outs

| Ruling | Spec paragraph | Parity-doc note | Follow-up naming the later work |
|---|---|---|---|
| C-1 structured streaming (3 cells, 2026-09-19) | "Carved out" ¶1 (lines 221–222) | SES-DECL rows (lines 3271, 3286) + C-1 citations added 2026-09-27 (F-4) | [ice-streaming-1-6.md](ice-streaming-1-6.md), card ICE-STREAMING, v1.6.0 |
| C-2 `D-NS-NESTED` (2026-09-24) | U5 owner item (lines 133–135) + "Carved out" ¶2 (lines 224–225) | §NS-2 (line 1478) + C-2 citation added 2026-09-27 (F-5) | **FINDING F-6:** the spec promises "the v1.6.0 card"; no card file names nested namespaces (only the spec, the [runs-28/29 orchestrating note](night-report-2026-09-23-29-orchestrating-note.md) line 20, and a pre-existing-residue mention in `task/ledgers/staging/ice-nested-evo-1-ledger.md` line 628). Not fixed: a new card exceeds this round's edits. |
| C-3 `P-RDF-PARTIAL-PROGRESS` (2026-09-26, rule `commit_order`) | "Carved out" ¶3 (lines 227–233) | §ICE-RDF-OPTIONS-1 (line 9114) | None — terminal by ruling: Spark's order is JVM-identity-hash dependent, so no deterministic order can match it; the rule is the permanent answer. No finding. |
| C-4 `TY-VARIANT-V3` (2026-09-27) | U9 (line 175) + "Carved out" ¶4 (lines 235–243) | §V3-VARIANT-SHRED-1 residue (line 2796) | [ice-variant-1-6.md](ice-variant-1-6.md), card ICE-VARIANT, v1.6.0 |
| C-5 `L-INSERT-OVERWRITE` (2026-09-27, rule `row_id_unordered`) | U0 row (line 65) + "Carved out" ¶5 (lines 245–254) | row-lineage residue (line 10254) | `task/ledgers/staging/r-fileorder-2-ledger.md` R-1 (re-measure against a future Spark before lifting) and R-3 (the rule hides order-only regressions) |

## 3. Spec units U0 to U12

PR numbers come from the [runs-28/29 orchestrating note](night-report-2026-09-23-29-orchestrating-note.md),
the [09-25](day-report-2026-09-25-direct.md) and [09-26](day-report-2026-09-26-direct.md) day reports,
and `git log origin/main --oneline`; hashes are the squash-merge commits on `origin/main`. All
units except U12 replay EQUAL (or both-refuse SPARK-CANNOT) on the 2026-09-27 02:25 matrix,
confirmed by the evening rerun on `f7422565` (same verdicts — see the closing note).

| Unit | Landed by | Spec still reads open? |
|---|---|---|
| U0 in flight (22 cells) | #805 `0002a7f2` (R-DF-LOAD-META ×2), #806 `fd43f192` (R-MT-DESCRIBE), #807 `07a98452` + #821 `4769ceac` (orphans), views PR2–PR6 #812 `d1a70b7d` / #815 `c7879a91` / #818 `a6e8bcda` / #823 `54e2da6a` / #825 `a969a5b5`, #854 `f6e4a949` (R-MC-ROW-ID-V3), L-INSERT-OVERWRITE by C-5; R-MT-FILES replays EQUAL with no dedicated RP-48 commit on main (no `rp-48` in `git log 2c7a4d25..origin/main`; the fork #345 content arrived through a later repin) | Yes — "next step" column and open PR refs (lines 58–66). Stale (F-2); dated pointer note added, no rewrite. |
| U1 MEM-LAYOUT | #814 `2de327bb` + #820 `4b1688f2` with fork #347 (Hadoop metadata names) | Yes — design proposal + "flagged to the owner" (lines 70–78). Stale (F-2); pointer note added. |
| U2 PROC-NORMALISE | Owner ruling U2 2026-09-23, no code PR: per-key `normalise` entries in `overrides.json` (`epoch_ms_rank`, `snapshot_ref`, `stats_file`, `staging_id` under ruling R6) | Yes — "Proposal … the owner rules" (lines 89–92). Stale (F-2); pointer note added. |
| U3 PD-BATTERY | Fixed by the spec's own base: `e38ad896` (startswith) + `66252e20` (decimal→DOUBLE widen), both in main `88b6f59f`; the `overrides.json` PD-IDENT/PD-MULTI notes confirm | Yes — "RePark refuses two shapes … Closes 9 cells" (lines 96–102). Stale at birth (F-2); pointer note added. |
| U4 SHOW/DESCRIBE | #810 `970ac11a` + #813 `458718b5` + #816 `d4caca39`, then #852 `4c5c2be8` (TBLPROPS-1) | Yes — "refused / not parsed" table (lines 106–115). Stale (F-2); pointer note added. |
| U5 ALTER/nested + tail | PR1 `6cf215bd`, PR2a #831 `64735038`, PR2b #834 `78d7d85d`, PR3 `4ae73c2c`; D-NS-NESTED by C-2 | Yes — work list + owner item (lines 122–135). Stale (F-2); pointer note added. |
| U6 WRITE-REFUSALS | U6 PR1 `fb41309f`; eight cells moved to both-refuse SPARK-CANNOT ([09-25 report](day-report-2026-09-25-direct.md) §Scoreboard) | Yes — "The fix is refusal parity" as proposal (lines 141–148). Stale (F-2); pointer note added. |
| U7 WRITE-DF | #830 `9e3bf2dd` (PR1), #835 `f3242566` (PR2 slice 1), `b55dc825` (PR2 slice 2; open as #837 in the 09-25 report) | Yes — "RePark today" refusals (lines 154–158). Stale (F-2); pointer note added. |
| U8 WRITE-SQL | #833 `e97682ed` (PR1), `60eaa729` (PR2 nested assignment) | Yes — open cell list (lines 182–186). Stale (F-2); pointer note added. |
| U9 TYPES | PR1 `f9db8472` (LTZ + empty map), PR2 #844 `5a1c8ebd` (VOID + UUID), NTZ-1 slice 1 #856 `f09b44e5`; TY-VARIANT-V3 by C-4 | Partly — the C-4 line (158) is current; the rest reads open (lines 173–178). Stale (F-2); pointer note added. |
| U10 READ-REST | #811 `b7a3c905` (input_file_name), #819 `3cf263da` + #822 `bf90513a` (path loads, _deleted); REG-1 was a harness timing race, no PR ([run-28 opus58 report](day-report-2026-09-23-28-opus58.md) lines 8, 73–78) | Yes — "REG-1 first … Then …" (lines 182–186). Stale (F-2); pointer note added. |
| U11 PROPS/EDGE/CATALOG | #843 `4c8e6633` (U11-EDGE-1), #851 `fbd97ef2` + #855 `1a219450` (CATALOG-1), #857 `9aa1c185` (RP-54: manifest merge, delete granularity), #858 `e3e35b0c` (TZ-ASOF-1) | Yes — "RePark today" table (lines 190–199). Stale (F-2); pointer note added. Decision 7 closed by the 2026-09-26 owner ruling (type=memory refuses at first use, `repark.sql.catalogExtensions` opt-in). |
| U12 S3-PATH-WRITE | **Nothing:** no `W-PATH-S3-*` cells in `matrix.json` (still 842 cells), no commits, no day-report mention since the card | The spec (§U12, lines 203–217) and the [card](s3-path-write-1-5-0.md) (line 10, quoted at audit time; the card now carries the 2026-09-27 owner note) read as open in-target work — and the work is unstarted. **FINDING F-1:** owner ruled 2026-09-27: U12 returns to v1.5.1; dated notes added to the spec and the card. |

Spec §2 decisions: 1 (Q-55-7, landed #814 under the run-29 override grant), 2 (U2 overrides,
owner ruling 2026-09-23 in `overrides.json`), 3 (C-2), 4 (U6 refusal parity, override-grant
ruling in the runs-28/29 note), 5 (TZ cells EQUAL via `e3e35b0c`, `f09b44e5`, `f9db8472`; no
separate dated TZ ruling found in the day reports), 6 (C-4, already marked closed), 7
(CAT-TYPE-MEMORY, 2026-09-26 owner ruling per the 09-26 report). All read open except 6 —
stale (F-3); dated pointer note added.

## 4. `docs/release.md` "Hard blockers" and "Open items" — report only

Not edited, per the work order. Read 2026-09-27 against the tree.

| Item (`docs/release.md` lines) | Still holds today? |
|---|---|
| Hard blockers: #95 RESOLVED 2026-08-14; "No hard blocker remains" (lines 57–66) | Yes. The import smoke is live in `.github/workflows/release.yml` (lines 76–77: `import repark.sql` must fail). No new blocker is recorded anywhere in the work order's inputs. |
| Open: wheel matrix — musllinux and further architectures stay open (lines 107–108) | Yes, still open. `release.yml` carries exactly the settled five-leg abi3 matrix (`ubuntu-latest`, `ubuntu-24.04-arm`, `macos-latest`, `macos-15-intel`, `windows-latest`, lines 17–31); no musl leg. |
| Open: cadence — minors carry features, patches carry fixes; 1.0.1 the first patch (lines 109–111) | Yes. Last tag `2c7a4d25` (2026-09-15) is v1.4.2, the second patch on 1.4.0 — the cadence reads unchanged. |
| Open: signing / attestation — PyPI attestations free with trusted publishing; Sigstore for crates and GitHub artifacts still open (lines 112–114) | Yes, still open. No attest or Sigstore step in `release.yml`; no intervening decision in the work order's inputs. |

## 5. Open follow-ups that do NOT block v1.5.0

| Follow-up | Record that names it |
|---|---|
| NTZ-1 slices 2 (store assignment) and 3 (storage surface + TZ-6) | `task/ledgers/staging/ntz-1-ledger.md` C-006–C-009 OPEN (lines 44–47) and [day-report-2026-09-26-direct.md](day-report-2026-09-26-direct.md) "Next" (line 146) |
| The `uuid_cast.rs` byte-offset window bug | [day-report-2026-09-26-direct.md](day-report-2026-09-26-direct.md) "Next" (lines 148–149: "a card for the … bug" — proposed, no card file yet) |
| CASESENS-1 (namespace-case R-6 plus the U11-EDGE residues) | `task/ledgers/staging/u11-edge-1-ledger.md` R-5–R-17 (lines 175–187), `task/ledgers/staging/catalog-1-ledger.md` R-6 (line 66), and the 09-26 "Next" (line 147) |
| CAT-TYPE-MEMORY three-part upper case (`SELECT * FROM C_MEM.n1.t` text differs) | `task/ledgers/staging/catalog-1-ledger.md` R-7 (line 67) and the 09-26 "Next" (line 149) |
| The C-4 variant build | [ice-variant-1-6.md](ice-variant-1-6.md), card ICE-VARIANT, v1.6.0 |
| The C-1 streaming card | [ice-streaming-1-6.md](ice-streaming-1-6.md), card ICE-STREAMING, v1.6.0 |
| U12 S3 path writes | [s3-path-write-1-5-0.md](s3-path-write-1-5-0.md), card S3-PATH-WRITE-1, v1.5.1 (owner ruling 2026-09-27) |

## 6. Findings

| # | Finding | File to fix | Fixed this round? |
|---|---|---|---|
| F-1 | U12 (S3 path writes) is unstarted while the spec and card still gated v1.5.0 on it at audit time | the spec + card | Yes — owner ruling 2026-09-27: U12 returns to v1.5.1; dated notes in both |
| F-2 | Spec §1 units U0–U11 read as open work though all landed (matrix green; confirmed by the 2026-09-27 evening rerun on `f7422565`) | `task/roadmap/mid-term/v1-5-0-remainder-spec-2026-09-23.md` | Yes — dated pointer note, no rewrite, no deletion |
| F-3 | Spec §2 decisions 1, 2, 4, 5, 7 read open though all closed | same spec | Yes — dated pointer note |
| F-4 | SES-DECL-readStream/streams rows lack the C-1 carve-out citation and card link | `docs/spark-sql-iceberg-parity.md` | Yes — dated pointer notes on both rows |
| F-5 | NS-2 row lacks the C-2 carve-out citation | `docs/spark-sql-iceberg-parity.md` | Yes — dated pointer note |
| F-6 | No v1.6.0 follow-up card for C-2 nested namespaces, though the spec promises "the v1.6.0 card" | [ns-nested-1-6.md](ns-nested-1-6.md) | Yes — card filed 2026-09-27 (WO NS-NESTED-CARD) |
