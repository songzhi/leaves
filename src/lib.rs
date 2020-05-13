pub use dao::LeafDao;
pub use error::{Error, Result};
pub use segment::LeafSegment;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
compile_error!("one of 'runtime-async-std' or 'runtime-tokio' features must be enabled");

#[cfg(all(feature = "runtime-tokio", feature = "runtime-async-std"))]
compile_error!("only one of 'runtime-async-std' or 'runtime-tokio' features must be enabled");

pub mod dao;
pub mod error;
pub mod segment;

#[derive(Debug, Copy, Clone)]
#[cfg_attr(
    any(feature = "mysql", feature = "postgres", feature = "sqlite"),
    derive(sqlx::FromRow)
)]
/// data stored in DB
pub struct Leaf {
    /// unique identifier
    pub tag: i32,
    pub max_id: i64,
    /// step when updating `max_id`
    pub step: i32,
}
