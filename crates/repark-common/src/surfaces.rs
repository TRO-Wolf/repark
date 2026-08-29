//! Dialect-neutral SQL surface registry audited by both doors (design `docs/design/sql-doors.md`
//! §2 Q13, graft G2).
//!
//! IDs name capabilities rather than syntax, and each door maps every ID to a [`Row`].
//! [`audit`] rejects missing, unknown, duplicate, or untraceable rows.
//!
//! This tier-0 module lets both doors depend on the registry without a door-to-door edge.

use std::collections::BTreeSet;

/// A stable, dialect-neutral name for one engine SQL capability.
/// The newtype prevents matrix rows from using arbitrary strings.
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

/// Generates surface-ID constants and [`ALL`] from one list, preventing drift.
/// `stringify!` derives each wire name from its constant identifier.
macro_rules! surface_ids {
    ($($(#[$meta:meta])* $konst:ident;)+) => {
        $($(#[$meta])* pub const $konst: SurfaceId = SurfaceId(stringify!($konst));)+

        /// Every surface ID in declaration order.
        /// [`audit`] requires each door matrix to map exactly this set.
        pub const ALL: &[SurfaceId] = &[$($konst),+];
    };
}

surface_ids! {
    // --- Statement forms ---
    /// A plain `SELECT` reaching the engine unchanged (the delegation baseline).
    SELECT_PASSTHROUGH;
    /// `CREATE TABLE … AS SELECT` onto a staged Iceberg create/replace transaction.
    CTAS;
    /// How a CTAS target resolves to a catalog, including loud refusal when it does not.
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
    /// Snapshot-pinned reads using `VERSION` or `TIMESTAMP AS OF`, with or without `FOR`.
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
    /// Data-file format option. Unsupported ORC and AVRO formats refuse loudly (design §0 graft G9).
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
    /// Unknown bare creation-option keys refuse loudly and list the curated set (design §2 Q1).
    TABLE_OPTION_UNKNOWN_KEY_REFUSE;
    /// Partition-transform validation — names, argument counts, bounds (design §2 Q2).
    PARTITION_TRANSFORM_VALIDATION;
    /// Creating a merge-on-read table (reaching `write.*.mode` at create time).
    MOR_TABLE_CREATION;
    /// The namespace-location option on `CREATE SCHEMA`.
    SCHEMA_OPTION_LOCATION;

    // --- Guard rails (design §2 Q12) ---
    /// Quote-aware multi-statement SQL refusal, checked first in the router.
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
    /// After a parse or plan failure, wrong-door sniff names the token, equivalent, and other door (design §2 Q10 / graft G3).
    WRONG_DOOR_SNIFF;
    /// Identifier case folding, and where it diverges between the doors (design §2 Q10).
    IDENTIFIER_CASE_FOLDING;
    /// Technical-analysis functions reaching SQL (design §2 Q11).
    TA_FUNCTIONS;
    /// The frozen `SqlDialect::execute` seam — the door is reachable through a session (§3).
    SQL_DIALECT_SEAM;
    /// Cross-door result equivalence (design §2 Q13 / graft G5).
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
/// Session profile for matrix evidence (design §2 Q13, graft G5).
///
/// Explicit profiles prevent Spark-extended evidence from being misread as native evidence.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionProfile {
    /// An in-process unit test without a session or extension.
    Unit,
    /// A session without extensions and with native/ANSI expression semantics.
    Native,
    /// A session with the Spark extension installed.
    SparkExtended,
    /// Separate native and Spark-extended sessions use their own doors for Arrow value and type comparison.
    /// One Spark-extended session cannot prove equivalence because its extensions affect every door.
    TwoSession,
}

/// ===========================================================================================
/// One door's answer for one surface.
///
/// Each surface is either `Tested` or `DeliberatelyAbsent`. Missing rows fail the audit.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Row {
    /// The door implements this surface.
    Tested {
        /// The pinning test's `cargo test -- --list` name.
        test: &'static str,
        /// The session profile the evidence was gathered under.
        profile: SessionProfile,
    },
    /// The door does not implement this surface.
    DeliberatelyAbsent {
        /// Why the door lacks this surface and what would reopen it (design §6 R5).
        reason: &'static str,
        /// The design section or ADR that decided it.
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
/// Audits one door matrix against [`ALL`]. Missing, unknown, duplicate, and untraceable rows fail.
/// ===========================================================================================
///
/// # Errors
///
/// Returns a newline-joined message listing every problem.
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
