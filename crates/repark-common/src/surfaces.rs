//! The dialect-neutral SQL **surface registry** — the typed vocabulary both doors are audited
//! against (design `docs/design/sql-doors.md` §2 Q13, graft G2).
//!
//! One const ID list lives here, at tier 0, because it is a statement about the PRODUCT surface,
//! not about either dialect: "the engine has a CTAS surface" is true regardless of which door
//! spells it. IDs are therefore named by CAPABILITY, never by spelling — one ID covers the ANSI
//! `WITH (format_version = 2)` form and the Spark `TBLPROPERTIES` form.
//!
//! Each door carries its own `matrix.rs` mapping **every** ID in [`ALL`] to a [`Row`] — either
//! [`Row::Tested`] naming the test that pins it and the session it ran under, or
//! [`Row::DeliberatelyAbsent`] naming the reason and the design/ADR section that decided it —
//! with a compile-run audit test ([`audit`]) that FAILS on an unmapped, unknown, duplicated or
//! untraceable row. That is what makes **absence typed and build-enforced**: a surface cannot
//! quietly not exist, and a new ID cannot land without both doors answering for it.
//!
//! Adding an ID is therefore a deliberate act with a cost: it reds both doors until each maps
//! it. The module lives in `repark-common` (tier 0) so both tier-3 doors reach it without a
//! door→door edge (design §1: "no door→door edge, ever").

use std::collections::BTreeSet;

/// A surface identifier: a stable, dialect-neutral name for one engine SQL capability.
///
/// A newtype rather than a bare `&str` so a matrix row cannot be keyed by an arbitrary string
/// that merely looks like an ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SurfaceId(&'static str);

impl SurfaceId {
    /// The identifier's stable name (used in audit-failure messages).
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for SurfaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Declare the surface-ID constants and the [`ALL`] slice from one list, so the two can never
/// drift (a constant that is not in `ALL` is unauditable; an `ALL` entry with no constant is
/// unreferenceable). The wire name is `stringify!`d from the constant's own identifier, so the
/// third drift class — a constant whose string still spells the surface it was copied from —
/// cannot be written either.
macro_rules! surface_ids {
    ($($(#[$meta:meta])* $konst:ident;)+) => {
        $($(#[$meta])* pub const $konst: SurfaceId = SurfaceId(stringify!($konst));)+

        /// Every surface ID, in declaration order (grouped by family: statement forms,
        /// table-creation options, guard rails, ergonomics + seams, value semantics).
        /// **The audit's universe** — [`audit`] fails a door whose matrix does not map
        /// exactly this set.
        pub const ALL: &[SurfaceId] = &[$($konst),+];
    };
}

surface_ids! {
    // --- Statement forms ---
    /// A plain `SELECT` reaching the engine unchanged (the delegation baseline).
    SELECT_PASSTHROUGH;
    /// `CREATE TABLE … AS SELECT` onto a staged Iceberg create/replace transaction.
    CTAS;
    /// How a CTAS target name resolves to a catalog — and what happens when it does not
    /// (design §2 Q15 / graft G1: loud refuse, never a silent `MemTable`).
    CTAS_TARGET_ROUTING;
    /// `CREATE OR REPLACE TABLE … AS SELECT` (the staged replace).
    CREATE_OR_REPLACE_TABLE;
    /// `CREATE TABLE t (cols …)` — schema-only staged create, no query.
    CREATE_TABLE_COLUMN_DEF;
    /// `DROP TABLE`, with `IF EXISTS` idempotency.
    DROP_TABLE;
    /// `CREATE SCHEMA` / `CREATE NAMESPACE` (dialect spellings of one capability).
    CREATE_SCHEMA;
    /// `DROP SCHEMA` / `DROP NAMESPACE`, with `IF EXISTS` idempotency.
    DROP_SCHEMA;
    /// `ALTER TABLE … RENAME TO`.
    ALTER_TABLE_RENAME;
    /// Schema evolution from SQL (`ALTER TABLE … ADD/DROP/RENAME COLUMN`, type widening).
    ALTER_TABLE_SCHEMA_EVOLUTION;
    /// Table-property mutation from SQL (`ALTER TABLE … SET/UNSET PROPERTIES`).
    ALTER_TABLE_PROPERTIES;
    /// Partition-spec evolution from SQL (design §2 Q3).
    ALTER_TABLE_PARTITION_FIELDS;
    /// `INSERT INTO`.
    INSERT_INTO;
    /// `INSERT OVERWRITE` (design §2 Q9).
    INSERT_OVERWRITE;
    /// `DELETE FROM`.
    DELETE;
    /// `UPDATE … SET`.
    UPDATE;
    /// `MERGE INTO` (design §2 Q4).
    MERGE;
    /// `TRUNCATE TABLE`.
    TRUNCATE;
    /// Snapshot-pinned reads (design §2 Q5) — `VERSION`/`TIMESTAMP AS OF`, with or without
    /// `FOR`.
    TIME_TRAVEL;
    /// Branch / tag DDL (design §2 Q6).
    BRANCH_TAG_DDL;
    /// Statement-shaped maintenance (`CALL … system.…`, `ALTER TABLE … EXECUTE`) — §2 Q7.
    MAINTENANCE_CALL;
    /// Iceberg metadata tables (`t$snapshots` / `t.snapshots`).
    METADATA_TABLES;
    /// Catalog introspection: `SHOW` / `DESCRIBE` / `information_schema` (design §2 Q8).
    INTROSPECTION;

    // --- Table-creation options ---
    /// The data-file format option (`format = 'PARQUET'` / `USING parquet`); ORC + AVRO refuse
    /// loud naming the trigger (design §0 graft G9).
    TABLE_OPTION_FORMAT;
    /// The Iceberg format-version option.
    TABLE_OPTION_FORMAT_VERSION;
    /// The partition-spec option (`partitioning = ARRAY[…]` / `PARTITIONED BY (…)`).
    TABLE_OPTION_PARTITIONING;
    /// The table-location option.
    TABLE_OPTION_LOCATION;
    /// The raw-Iceberg-key escape hatch (`extra_properties = MAP(…)` / `TBLPROPERTIES`) — G4.
    TABLE_OPTION_RAW_PROPERTIES;
    /// The sort-order option (`sorted_by`) — held as a loud refuse naming its trigger (G9).
    TABLE_OPTION_SORT_ORDER;
    /// The typo guard: an unknown BARE creation-option key refuses loud, listing the curated
    /// set (design §2 Q1).
    TABLE_OPTION_UNKNOWN_KEY_REFUSE;
    /// Partition-transform validation — names, argument counts, bounds (design §2 Q2).
    PARTITION_TRANSFORM_VALIDATION;
    /// Creating a merge-on-read table (reaching `write.*.mode` at create time).
    MOR_TABLE_CREATION;
    /// The namespace-location option on `CREATE SCHEMA`.
    SCHEMA_OPTION_LOCATION;

    // --- Guard rails (design §2 Q12) ---
    /// Multi-statement SQL refuses — quote-aware, and FIRST in the router (the ordering-defect
    /// fix mandated by the design judges, §0).
    GUARD_MULTI_STATEMENT;
    /// P11: DML against a read-only catalog refuses with the generic message.
    GUARD_READ_ONLY_CATALOG;
    /// SEC-02: a plan that would read or write the local filesystem refuses.
    GUARD_LOCAL_FILESYSTEM;
    /// Writes targeting a branch/tag ref refuse (the v1 valve; a fork gap, not a design choice).
    GUARD_WRITE_TO_BRANCH;
    /// BUG-001: DML on an unpartitioned-after-evolution merge-on-read table refuses.
    GUARD_MOR_MULTI_SPEC_DML;

    // --- Ergonomics + seams ---
    /// The error-path-only wrong-door sniff: on parse/plan FAILURE, name the token, the native
    /// equivalent, and the other door (design §2 Q10 / graft G3).
    WRONG_DOOR_SNIFF;
    /// Identifier case folding, and where it diverges between the doors (design §2 Q10).
    IDENTIFIER_CASE_FOLDING;
    /// Technical-analysis functions reaching SQL (design §2 Q11).
    TA_FUNCTIONS;
    /// The frozen `SqlDialect::execute` seam — the door is reachable through a session (§3).
    SQL_DIALECT_SEAM;
    /// Two-session cross-door result equivalence (design §2 Q13 / graft G5). A single-session
    /// `sql_with` row does NOT discharge this — extensions are session-scoped, so a
    /// Spark-extended session has Spark expression semantics through every door.
    CROSS_DOOR_EQUIVALENCE;

    // --- Value semantics (H-2 G8) ---
    /// `ORDER BY` / window `ORDER BY` default null placement (`NULLS FIRST` vs `LAST`).
    SEMANTICS_NULL_ORDERING;
    /// Decimal arithmetic: result `(p,s)`, the 38-digit clamp, and bit-exact `i128` payloads.
    SEMANTICS_DECIMAL_ARITHMETIC;
    /// Cast / coercion value matrix (overflow, invalid input, timestamp-as-numeric).
    SEMANTICS_CAST_MATRIX;
    /// Session-timezone extraction (`spark.sql.session.timeZone`) vs stored-zone fallback.
    SEMANTICS_SESSION_TIMEZONE;
    /// Window frame bounds (`ROWS` / `RANGE`, including temporal `RANGE`).
    SEMANTICS_WINDOW_FRAMES;
    /// Join values when a key is NULL (NULL≠NULL on INNER; outer-join orphans).
    SEMANTICS_JOIN_NULL_KEYS;
    /// Float aggregation determinism across `target_partitions` (`f64::to_bits`).
    SEMANTICS_FLOAT_DETERMINISM;
}

/// ===========================================================================================
/// Which session a matrix row's evidence ran under (design §2 Q13, graft G5).
///
/// Recorded per row because the cross-door protocol is only meaningful when the profile is
/// explicit: a Spark-extended session has Spark expression semantics through EVERY door, so
/// "native" evidence gathered on an extended session proves nothing about the native door.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionProfile {
    /// An in-process unit test — no session, no extension (parsers, validators, refuse
    /// helpers).
    Unit,
    /// A session with NO extension installed: native/ANSI expression semantics.
    Native,
    /// A session with the Spark extension installed (functions + analyzer rules).
    SparkExtended,
    /// TWO sessions — one native, one Spark-extended — each driven through its OWN door,
    /// results compared on the Arrow path (value AND type). The only profile that discharges
    /// [`CROSS_DOOR_EQUIVALENCE`].
    TwoSession,
}

/// ===========================================================================================
/// How ONE door answers for ONE surface.
///
/// The two variants are the whole point: a door either pins the surface with a named test, or
/// states in the type system that it deliberately does not have it, with the reason and the
/// deciding design/ADR section attached. There is no third "unknown" or "not yet" state — an ID
/// with no row fails the door's audit test.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Row {
    /// The door implements this surface.
    Tested {
        /// The pinning test's `cargo test -- --list` name (e.g. `ctas::tests::writes_rows`).
        test: &'static str,
        /// The session profile the evidence was gathered under.
        profile: SessionProfile,
    },
    /// The door deliberately does NOT implement this surface.
    DeliberatelyAbsent {
        /// Why — in the door's own terms, not a shrug; names the equivalent and the trigger
        /// that would reopen it (design §6 R5).
        reason: &'static str,
        /// The design section / ADR that decided it (e.g. `sql-doors.md §2 Q9`).
        adr: &'static str,
    },
}

impl Row {
    /// `true` when the door ships the surface.
    #[must_use]
    pub fn is_tested(&self) -> bool {
        matches!(self, Row::Tested { .. })
    }
}

/// ===========================================================================================
/// Audit one door's matrix against [`ALL`]: every ID mapped exactly once, no unknown entries,
/// every row traceable.
///
/// Lives here (not duplicated per door) so the two doors are audited by the SAME rule. Each
/// door's `matrix.rs` test calls this with its own `(SurfaceId, Row)` table and its own name.
///
/// Four failure modes, reported together so one run names every problem: an ID in [`ALL`] with
/// no row (the surface silently went missing), a row naming an unknown ID (a stale row after a
/// rename), a duplicated ID (two rows disagreeing about one surface), and an untraceable row —
/// a [`Row::Tested`] with an empty `test`, or a [`Row::DeliberatelyAbsent`] with an empty
/// `reason`/`adr`. A row that cites nothing is indistinguishable from the oversight this
/// machinery exists to prevent.
/// ===========================================================================================
///
/// # Errors
///
/// A human-readable, newline-joined message naming the unmapped IDs, unknown IDs, duplicated
/// IDs and untraceable rows. The caller (the door's audit test) asserts on `Ok`.
pub fn audit(door: &str, rows: &[(SurfaceId, Row)]) -> Result<(), String> {
    let mut problems: Vec<String> = Vec::new();

    for id in ALL {
        let count = rows.iter().filter(|(row_id, _)| row_id == id).count();
        match count {
            1 => {}
            0 => problems.push(format!(
                "surface `{id}` has NO row in the {door} matrix — map it \
                 Tested{{test, profile}} or DeliberatelyAbsent{{reason, adr}}"
            )),
            n => problems.push(format!(
                "surface `{id}` has {n} rows in the {door} matrix — exactly one is required"
            )),
        }
    }

    let universe: BTreeSet<SurfaceId> = ALL.iter().copied().collect();
    for (id, row) in rows {
        if !universe.contains(id) {
            problems.push(format!(
                "the {door} matrix maps `{id}`, which is not a known surface ID \
                 (repark_common::surfaces::ALL)"
            ));
        }
        match row {
            Row::Tested { test, .. } if test.trim().is_empty() => {
                problems.push(format!("`{id}`: Tested with an empty test name"));
            }
            Row::DeliberatelyAbsent { reason, adr }
                if reason.trim().is_empty() || adr.trim().is_empty() =>
            {
                problems.push(format!(
                    "`{id}`: DeliberatelyAbsent needs BOTH a reason and an adr"
                ));
            }
            _ => {}
        }
    }

    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("\n"))
    }
}

#[cfg(test)]
mod tests;
