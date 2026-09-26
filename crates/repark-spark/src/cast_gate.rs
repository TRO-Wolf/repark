use datafusion::error::Result;
use datafusion::sql::sqlparser::ast::Statement;

pub(crate) fn rewrite_and_refuse_casts(sql: &str, statement: &mut Statement) -> Result<()> {
    crate::void_type::rewrite_cast_null_to_void(statement);
    crate::uuid_cast::refuse_uuid_cast(sql, statement)
}
