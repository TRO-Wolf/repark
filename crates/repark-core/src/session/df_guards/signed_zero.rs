use std::sync::Arc;

use datafusion::prelude::SessionContext;

mod literals;

pub(super) use literals::SignedZeroLiterals;

pub(super) fn install_literal_seat(context: &SessionContext) {
    context.add_analyzer_rule(Arc::new(SignedZeroLiterals));
}

#[cfg(test)]
mod tests;
