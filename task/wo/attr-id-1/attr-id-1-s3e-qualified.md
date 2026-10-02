# ATTR-ID-1 S3e: qualified names on join children and `alias()` frames go through `resolve` (fresh Muse session, guided; lane /tmp/xattr, branch feat/attr-id-1, head 496bbced)

Read first:
- AGENTS.md, from this clone;
- the work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: §1, §3.4–§3.7, §4 "S3", §5, §9 (**Q4**: the facade's Python-held join qualifiers go into `resolve`), §9d, §9e (**B3/B4**: a dotted token is qualifier plus name only when the qualifier matches a relation the plan carries; facade-held qualifiers are S3e's) and §9f;
- the S3d briefs `attr-id-1-s3d-drop-na.md` and `attr-id-1-s3d-resume.md`, and S3d's final hand-back `/tmp/muse-worker/xattr/20261001T182908Z/handback.json`;
- the ledger `task/ledgers/staging/attr-id-1-ledger.md`, especially R-4, the S3a ruling Q2 (R5, R9 and the qualified half of R12) and every other row that names S3e as owner.

## Task: the S3e family only
S3a–S3d bind unqualified names by attribute id. Qualified names still take the old paths: `t.v`, `F.col("t.v")`, `df["t.v"]`, a dotted token in `filter`/`orderBy`/`selectExpr` text, and qualified references after `alias("t")` and on join children. Move them onto `resolve`, passing the frame's qualifiers.
- **Where qualifiers live today:** `DataFrame._join_qualifiers` (`core.py` about 291), `replace_expr._assign_join_qualifiers` (about 226), `replace_expr._bind_qualified_column` (about 183), `column_fields._qualified_target` and `_raise_unresolved_name`, and the native `logical_column_qualifiers` and `requalify_join_sides`. The design names `resolve_qualified_display_names`; that name no longer exists. Find its current equivalent by grep and say what you found.
- **The rule (measure every bullet against Spark under both case rules):**
  - A qualifier matches a relation alias the plan carries, or one the facade holds for a join side or an `alias()` frame, under the live case rule. The qualifier and the name each fold under that rule.
  - A qualified name binds the positions under that qualifier only. One id binds; several ids give AMBIGUOUS_REFERENCE; no hit gives Spark's UNRESOLVED_COLUMN error class with its suggestion list.
  - A dotted token whose head matches no qualifier keeps main's path unchanged, for example a struct field (§9e B3/B4). Where a token matches both a qualifier and a struct column, measure it and pin it.
- **Residues to close, or name:**
  - R-4: `resolve` sees the relation only, not the facade's join qualifiers.
  - R5 (11 cells): under the sensitive rule, qualifier case is inverted on alias frames (`t` binds, `T` misses at plan build).
  - R9 (534 cells): qualified join-side misses are shaped UNRESOLVED.
  - The qualified half of R12: 16 qualified `F.col` engine fall-throughs.
  Each cell either becomes EQUAL or is named with a reason and an owner. Only S4 and follow-up cards remain as owners.
- **Delete** the helpers only this family used, in the same commit. Show by grep that each has no other caller, and list the deletions. Leave the `__REPARK_QCOL_` token and `_origin_plan_id`/`_origin_field` to S4; if this family was their last reader, name that in the hand-back so S4 can remove them.
- Never add an "unknown identity" branch (halt rule 3).
- **Rust-first (ruling R-S3d-1 still applies):** new matching, folding or qualifier logic goes in Rust behind a native call. Do not add fold tables or matching loops in Python. Leave `NameRule` itself unchanged; its Java convergence belongs to the unicode-case card.

## Gate (the replay, cell by cell against `main.json`)
- `make develop` in the slice. Run `replay.py` with S0's main engine name into `/tmp/oc-worker/direct/wo/attr-id-1/s3e/t-head-N`, then `compare.py`. Copy the S3d harness into `s3e/`; never edit an earlier slice's harness in place.
- **Run every replay in the foreground**, with `timeout` and `ulimit -v 67108864`. **Never detach.**
- **0 cells move away from Spark** against `main.json`, and also against the S3d head (496bbced).
- No earlier gains are lost: all 8,558 FIXED at b02dd8bf and the 2,676 S3d gains stay.
- Timing (§9d): like for like, at most 1.2x, median of 3. Report the time on FIXED cells separately.
- The 47-file neighbour sweep with `-n 8`. `bash /tmp/xattr/gate.sh` prints GATE GREEN.

## Proof
- Pins under both case rules, each measured against Spark with `/tmp/oc-worker/_lib/jvm-lock.sh /tmp/sparkenv/bin/python` (JAVA_HOME=/usr/lib/jvm/zulu-17-amd64). Batch the Spark cells. Pin each of these:
  - a self-join with `alias("a")`/`alias("b")`, then `a.v` and `b.v` in `select`, `filter`, `orderBy`, `drop` and `withColumn`;
  - a qualifier-case variant (`A.v`, `a.V`) in each rule;
  - a qualified miss (UNRESOLVED_COLUMN error class);
  - a qualified twin pair under one alias (AMBIGUOUS_REFERENCE);
  - a struct field whose head equals no qualifier (main's path);
  - a struct column named like a qualifier;
  - an `alias()` frame re-aliased (`df.alias("a").alias("b")`: `a.v` misses and `b.v` binds, if Spark agrees);
  - the case rule changed between building the join and binding.
- Mutations, each turning its pins red, then reverted with `git status` clean:
  - `resolve` ignores the facade-held qualifiers;
  - the qualifier is matched exactly under the insensitive rule;
  - a qualified name binds across every side, ignoring its qualifier.

## Halt when
- A cell EQUAL on main moves away from Spark.
- The round needs an unknown-identity branch.
- The like-for-like timing is above 1.2x.
- A deleted helper has a caller outside the family.
- Spark's measured semantics contradict a bullet above. Halt with the measurement; do not choose.

## Commit (commit first, and keep `handback.json` provisional)
- Commit: `feat(attr-id-1): qualified names on join children and alias frames bind by attribute id through resolve (S3e)`.
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
- Exactly one trailer: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
- **No code comments** in any source or config file, including TOML, docstrings on new functions and `noqa` prose. Moved code sheds its comments. Update `map.md` in lockstep. Extend the ledger.
- Every Rust file stays at or below 1,000 lines, and every Python file stays within its CAP-1 row. Never raise a ceiling; ratchet down in lockstep when a file shrinks.
- Hand-back: `/tmp/xattr/handback.json`. Start a fresh one. If you run low on steps, commit what you have and list exactly what is still owed.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s3e/`. Never touch `~/CodeRepos`. Never use AWS credentials. No `.github` changes. Do not push.
