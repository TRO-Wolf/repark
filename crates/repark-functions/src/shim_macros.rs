//! Crate-private declarative macros shared by the shim `ScalarUDFImpl`s.
//!
//! File-backed to keep the crate root within its source-size ceiling; the crate root re-exports it.

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
