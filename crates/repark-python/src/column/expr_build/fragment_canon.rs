use std::borrow::Cow;

use datafusion::error::Result;
use datafusion::execution::context::SessionState;

pub(crate) fn canonicalize_fragment(sql: &str, keep_verbatim: bool) -> Result<Cow<'_, str>> {
    match repark_spark::keyword_lower::rewrite_fragment_timestamp_ltz_literals(sql) {
        Cow::Borrowed(text) => {
            repark_spark::spark_literals::canonicalize_verbatim(text, keep_verbatim)
        }
        Cow::Owned(rewritten) => {
            match repark_spark::spark_literals::canonicalize_verbatim(&rewritten, keep_verbatim)? {
                Cow::Borrowed(_) => Ok(Cow::Owned(rewritten)),
                Cow::Owned(text) => Ok(Cow::Owned(text)),
            }
        }
    }
}

pub(crate) fn canonicalize_predicate<'a>(
    state: &SessionState,
    sql: &'a str,
    keep_verbatim: bool,
) -> Result<Cow<'a, str>> {
    if repark_spark::is_spark_door_session(state.config()) {
        canonicalize_fragment(sql, keep_verbatim)
    } else {
        repark_spark::spark_literals::canonicalize_verbatim(sql, keep_verbatim)
    }
}
