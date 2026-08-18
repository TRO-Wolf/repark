//! Crate-private declarative macros shared by the shim `ScalarUDFImpl`s.
//!
//! File-backed so the crate root stays a manifest: `scripts/check_lib_rs.py` counts every line of
//! `lib.rs`, and this crate root sat exactly at its 175-line ceiling. Moving the macro body here
//! is sanctioned out (1) of that gate — "move production code into a named module with pub use
//! re-exports" — and it is what let FN-GT2 X8 add `pub mod url;` without raising the ceiling.
//!
//! Call sites keep spelling it `crate::shim_udf_boilerplate!(…)`, unchanged: `lib.rs` re-exports
//! the macro at the crate root, so the path is the same one the macro had when it was declared
//! there.

/// The `name` / `signature` boilerplate every shim `ScalarUDFImpl` shares. Pairs with
/// `Signature::user_defined`, which defers coercion to each impl's `coerce_types`, so a single
/// overload accepts Spark's full input range instead of a fixed type list.
macro_rules! shim_udf_boilerplate {
    ($name_literal:literal) => {
        fn name(&self) -> &str {
            $name_literal
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
    };
}

pub(crate) use shim_udf_boilerplate;
