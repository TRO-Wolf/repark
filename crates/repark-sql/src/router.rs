//! The ANSI statement router.
//!
//! The order below is the design's, and each step earns its position:
//!
//! 1. **Text guards, multi-statement FIRST** ([`crate::guards`]). Refusing a script before
//!    anything else runs is what stops a second statement from being rewritten or sniffed — the
//!    ordering-defect class the design's judges called out explicitly.
//! 2. **The merge-on-read valve** ([`guards::refuse_mor_multi_spec_dml`], BUG-001) — async,
//!    because it has to load the target table's metadata, so it cannot ride the text-guard fn.
//!    Still at the router head, still before any parse (design §2 Q12 puts it in this guard set).
//! 3. **The pre-parse stage** — the productions stock sqlparser cannot reach at all (the R1
//!    spike named them): the `ALTER TABLE … EXECUTE` refusal, the branch/tag recognizer
//!    ([`crate::ref_ddl`]), the `SET PROPERTIES` rewrite ([`crate::alter`]), and the
//!    `FOR … AS OF` rewrite ([`crate::time_travel`]). Its position is the BUG-010 ordering rule
//!    in action: every one of these runs AFTER the multi-statement refuse, so no recognizer can
//!    ever see — or rewrite — the second statement of a script.
//! 4. **Parse, with stock DataFusion parser machinery on the Generic dialect** (design §2 Q14 —
//!    no bespoke `Dialect` impl in phase 2).
//! 5. **Statement match.** The Iceberg catalog DDL DataFusion cannot express (`CREATE TABLE`
//!    both forms, `DROP TABLE`, `CREATE SCHEMA`, `DROP SCHEMA`, `ALTER TABLE`), the `MERGE INTO`
//!    lowering, and the refuse set ([`crate::refusals`]: `INSERT OVERWRITE`, `CALL`, `TRUNCATE`).
//! 6. **Delegate.** Everything else is planned and executed by DataFusion, with the SEC-02
//!    local-filesystem guard between planning and execution. That includes reads of the fork's
//!    metadata tables (`t$snapshots`, `t$files`, …), which the fork's schema provider registers
//!    as real tables, and `INSERT`/`DELETE`/`UPDATE`, which the fork's `TableProvider` services
//!    (ADR-0003).
//!
//! There is deliberately **no pre-parse `$` passthrough**. An earlier revision short-circuited to
//! delegation whenever the scrubbed text contained a `$`, which routed `CREATE TABLE t AS SELECT
//! … FROM x$snapshots` past the Q15 target check and into DataFusion's own CTAS — a session-local
//! `MemTable` that reads back all session and is gone tomorrow, the exact failure graft G1
//! exists to forbid. The stock parser handles `$` in an identifier, so a metadata reference
//! reaches delegation through the ordinary `_ =>` arm and a metadata reference inside a CTAS
//! reaches its handler like any other query.
//!
//! On a parse OR plan failure — and only then — the error goes through the wrong-door sniff
//! ([`crate::sniff`]).

use std::borrow::Cow;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{ObjectType, Statement};
use repark_core::EngineContext;

use crate::{
    alter, create_table, guards, merge, ref_ddl, refusals, schema_ddl, sniff, time_travel,
};

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
    guards::refuse_mor_multi_spec_dml(&cx, sql).await?;

    // --- Pre-parse stage: the three productions stock sqlparser cannot reach (R1 spike). ---
    // Every one of them runs AFTER the multi-statement refuse, which is the BUG-010 ordering
    // rule: a recognizer must never see (or rewrite) the second statement of a script.
    if let Some(refusal) = refusals::recognize_alter_table_execute(sql) {
        return Err(refusal);
    }
    if let Some(ddl) = ref_ddl::try_parse_ref_ddl(sql) {
        return ref_ddl::execute_ref_ddl(&cx, ddl?).await;
    }
    let sql = match alter::rewrite_set_properties(sql) {
        Some(rewritten) => Cow::Owned(rewritten),
        None => Cow::Borrowed(sql),
    };
    // The `FOR … AS OF` rewrite registers ephemeral pinned relations on the session; they are
    // released again as soon as the statement has been planned, so a long-lived session neither
    // accumulates them nor shows them in `SHOW TABLES` / `information_schema.tables` (the
    // introspection surface this same PR enabled). The plan owns its provider, so the returned
    // `DataFrame` still collects after the name is gone.
    let mut pinned = time_travel::PinnedViews::default();
    let result = execute_time_travelled(&cx, &sql, &mut pinned).await;
    pinned.release(cx.ctx);
    result
}

/// The rest of the pipeline, from the `FOR … AS OF` rewrite onward. Split out purely so
/// [`execute`] can release `pinned` on EVERY exit path — including the `?` ones.
async fn execute_time_travelled(
    cx: &EngineContext<'_>,
    sql: &str,
    pinned: &mut time_travel::PinnedViews,
) -> Result<DataFrame> {
    let sql = match time_travel::prepare_time_travel_sql(cx, sql, pinned).await? {
        Some(rewritten) => Cow::Owned(rewritten),
        None => Cow::Borrowed(sql),
    };
    let sql: &str = &sql;

    let statement = match cx.ctx.state().sql_to_statement(sql, &PARSER_DIALECT) {
        Ok(statement) => statement,
        Err(err) => return Err(sniff::upgrade_error(sql, err)),
    };
    let DFStatement::Statement(statement) = statement else {
        // DataFusion's own parser extensions (COPY, CREATE EXTERNAL TABLE, …) — delegate, where
        // the SEC-02 guard sees the resulting plan.
        return delegate(cx, sql).await;
    };

    match statement.as_ref() {
        Statement::CreateTable(create) => create_table::execute_create_table(cx, create).await,
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            ..
        } => {
            schema_ddl::execute_create_schema(
                cx,
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
        } => schema_ddl::execute_drop_table(cx, names, *if_exists).await,
        Statement::Drop {
            object_type: ObjectType::Schema | ObjectType::Database,
            names,
            if_exists,
            cascade,
            ..
        } => schema_ddl::execute_drop_schema(cx, names, *if_exists, *cascade).await,
        Statement::AlterTable(alter) => alter::execute_alter_table(cx, alter).await,
        Statement::Merge(merge) => merge::execute_merge(cx, merge).await,
        // --- The refuse set (design §2 Q7 / Q9). ---
        Statement::Insert(insert) if insert.overwrite => {
            Err(refusals::insert_overwrite(&insert.table.to_string()))
        }
        Statement::Call(function) => Err(refusals::maintenance_call(&function.name.to_string())),
        Statement::Truncate(truncate) => Err(refusals::truncate(
            &truncate
                .table_names
                .first()
                .map_or_else(|| "<table>".to_string(), |target| target.name.to_string()),
        )),
        _ => delegate(cx, sql).await,
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

#[cfg(test)]
mod tests;
