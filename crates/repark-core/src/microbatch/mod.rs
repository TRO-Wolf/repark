#![forbid(unsafe_code)]

pub mod driver;
#[cfg(test)]
mod foreach_tests;
pub mod progress;
mod run;
#[cfg(test)]
pub(crate) mod testing;
