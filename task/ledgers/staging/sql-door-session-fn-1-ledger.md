# Unit ledger — SQL-DOOR-SESSION-FN-1 · session functions on the Spark SQL door

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when SQL-DOOR-SESSION-FN-1 merges, or when the owner closes the slate row.

**Unit:** SQL-DOOR-SESSION-FN-1 · **Date:** 2026-09-06 · **Executor:** Muse Spark (muse-spark-1.3), Actor ·
**Branch:** `fix/sql-door-session-fn-1` · **Base:** `origin/main` `e100f72d` (v1.1.1)
**Model:** muse-spark-1.3
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2 (uv.lock pin), zulu-17, `local[2]`, ANSI on, 2026-09-06.
Banner proof is the measured `SELECT version()` cell below. Session-timezone banner omitted:
every cell here is a timezone-free string identity function.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Four cells (`user()`, `current_user()`, `session_user()`, `version()`) plus bare forms, expression/WHERE/FROM-`t` forms, shadowing and arity probes measured on both repark doors before the fix and on live Spark. | Oracle tables below. | **OPEN** |
| C-002 | Spark-door UDFs answer the facade strings: the three user spellings answer `repark` (equal to `F.current_user()`), `version()` answers `repark-<workspace>` (equal to `F.version()`); Utf8 non-null; native door untouched. | Kernels + registration + pins. | **OPEN** |
| C-003 | The Spark door accepts the parenthesised user calls (top level, inside expressions, WHERE, FROM-`t`) via a Databricks retry gated on a token sniff; bare forms keep today's column-or-error behavior; a retry that also fails returns the original Generic error. | Router/passthrough + pins. | **OPEN** |
| C-004 | Pins per form (Rust parse+evaluate, facade Spark-door cells+expressions, live shape pin) are red before the fix and green after; unregister mutation reds the suite. | Pin files + mutation table + pytest summaries. | **OPEN** |
| C-005 | Registry rows `SQL-SESSION-FN-1` and `SQL-VERSION-1` filed with per-door cells and the bare-form/native residues; maps lockstep; attestation with pytest counts. | Registry + maps + attestation. | **OPEN** |

## Oracle (live PySpark 4.1.2, 2026-09-06, JDK 17, `local[2]`, ANSI on)

TBD.
