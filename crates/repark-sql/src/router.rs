//! The ANSI statement router.
//!
//! The order below is the design's, and each step earns its position:
//!
//! 1. **Text guards, multi-statement FIRST** ([`crate::guards`]). Refusing a script before
//!    anything else runs is what stops a second statement from being rewritten or sniffed — the
//!    ordering-defect class the design's judges called out explicitly.
//! 2. **Metadata (`$`) passthrough.** The fork's schema provider already registers
//!    `t$snapshots`, `t$files`, … as real tables, so a metadata query needs no interception at
//!    all — it needs to be kept AWAY from the statement match, whose `CREATE`/`DROP` arms have
//!    nothing to say about it.
//! 3. **Parse, with stock DataFusion parser machinery on the Generic dialect** (design §2 Q14 —
//!    no bespoke `Dialect` impl in phase 2).
//! 4. **Statement match.** Only the Iceberg catalog DDL DataFusion cannot express is
//!    intercepted: `CREATE TABLE` (both forms), `DROP TABLE`, `CREATE SCHEMA`, `DROP SCHEMA`.
//! 5. **Delegate.** Everything else is planned and executed by DataFusion, with the SEC-02
//!    local-filesystem guard between planning and execution.
//!
//! On a parse OR plan failure — and only then — the error goes through the wrong-door sniff
//! ([`crate::sniff`]).
//!
//! `FOR VERSION|TIMESTAMP AS OF` time travel is **PR-6**, not here: the R1 spike confirmed the
//! stock parser cannot reach it, so it needs the token-scan rewrite that PR-6 lands. Until then a
//! `FOR … AS OF` statement fails to parse and the wrong-door sniff explains the spelling.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{ObjectType, Statement};
use repark_core::EngineContext;

use crate::scan::blank_out_quoted_and_comments;
use crate::{create_table, guards, schema_ddl, sniff};

/// The dialect handed to DataFusion's parser. Stock `Generic`, deliberately — NOT DataFusion's
/// `Ansi` dialect, which is untested against the phase-1 baseline and would be a silent
/// regression surface (design §2 Q14).
const PARSER_DIALECT: datafusion::config::Dialect = datafusion::config::Dialect::Generic;

/// ===========================================================================================
/// Execute one ANSI SQL statement against an [`EngineContext`].
/// ===========================================================================================
///
/// # Errors
/// A guard refusal, a parse/plan failure (upgraded by the wrong-door sniff when it recognizes a
/// Spark-ism), or any iceberg / execution error from an intercepted handler.
pub async fn execute(cx: EngineContext<'_>, sql: &str) -> Result<DataFrame> {
    guards::run_text_guards(&cx, sql)?;

    // Metadata tables are already real tables on the fork's schema provider — delegate directly
    // rather than letting the statement match consider them.
    if references_metadata_table(sql) {
        return delegate(&cx, sql).await;
    }

    let statement = match cx.ctx.state().sql_to_statement(sql, &PARSER_DIALECT) {
        Ok(statement) => statement,
        Err(err) => return Err(sniff::upgrade_error(sql, err)),
    };
    let DFStatement::Statement(statement) = statement else {
        // DataFusion's own parser extensions (COPY, CREATE EXTERNAL TABLE, …) — delegate, where
        // the SEC-02 guard sees the resulting plan.
        return delegate(&cx, sql).await;
    };

    match statement.as_ref() {
        Statement::CreateTable(create) => create_table::execute_create_table(&cx, create).await,
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            ..
        } => {
            schema_ddl::execute_create_schema(
                &cx,
                schema_object_name(schema_name)?,
                *if_not_exists,
                with.as_ref(),
            )
            .await
        }
        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            if_exists,
            ..
        } => schema_ddl::execute_drop_table(&cx, names, *if_exists).await,
        Statement::Drop {
            object_type: ObjectType::Schema | ObjectType::Database,
            names,
            if_exists,
            cascade,
            ..
        } => schema_ddl::execute_drop_schema(&cx, names, *if_exists, *cascade).await,
        _ => delegate(&cx, sql).await,
    }
}

/// Plan with DataFusion, run the SEC-02 guard on the resulting plan, then execute.
///
/// The guard sits BETWEEN planning and execution because that is the only place the target of a
/// `COPY TO` / `CREATE EXTERNAL TABLE` is known as data rather than as text.
async fn delegate(cx: &EngineContext<'_>, sql: &str) -> Result<DataFrame> {
    let plan = match cx.ctx.state().create_logical_plan(sql).await {
        Ok(plan) => plan,
        Err(err) => return Err(sniff::upgrade_error(sql, err)),
    };
    guards::refuse_local_filesystem_plan(cx.ctx, cx.catalogs, &plan)?;
    cx.ctx.execute_logical_plan(plan).await
}

/// A `CREATE SCHEMA`'s name, which sqlparser models as either a plain name or a `<name>
/// AUTHORIZATION <user>` pair.
fn schema_object_name(
    schema_name: &datafusion::sql::sqlparser::ast::SchemaName,
) -> Result<&datafusion::sql::sqlparser::ast::ObjectName> {
    use datafusion::sql::sqlparser::ast::SchemaName;
    match schema_name {
        SchemaName::Simple(name) => Ok(name),
        SchemaName::UnnamedAuthorization(_) | SchemaName::NamedAuthorization(_, _) => {
            Err(DataFusionError::NotImplemented(
                "CREATE SCHEMA … AUTHORIZATION is not supported — this engine has no schema \
                 ownership model"
                    .to_string(),
            ))
        }
    }
}

/// True when the statement references an Iceberg metadata table (`t$snapshots`, …). Checked on
/// scrubbed text so a `$` inside a string literal cannot route a statement away from its handler.
fn references_metadata_table(sql: &str) -> bool {
    blank_out_quoted_and_comments(sql).contains('$')
}

#[cfg(test)]
mod tests;
