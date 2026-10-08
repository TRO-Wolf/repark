#![forbid(unsafe_code)]

pub mod driver;
#[cfg(test)]
mod foreach_tests;
mod run;
#[cfg(test)]
pub(crate) mod testing;
