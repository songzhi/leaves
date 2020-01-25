//! Currently the mysql and postgres implementations are exactly the same.
//! I tried generic and macro, both failed.So this is it.
//!

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "redis")]
pub mod redis;
