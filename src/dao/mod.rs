//! Currently the mysql and postgres implementations are exactly the same.
//! I tried generic and macro, both failed.So this is it.
//!

use async_trait::async_trait;

use crate::{Leaf, Result};

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "mongo")]
pub mod mongodb;

#[cfg(test)]
pub mod mock;

#[async_trait]
pub trait LeafDao {
    /// get all leaves
    async fn leaves(&self) -> Result<Vec<Leaf>>;
    /// get a leaf by tag
    async fn leaf(&self, tag: i32) -> Result<Leaf>;
    /// create a new leaf
    async fn insert(&self, leaf: Leaf) -> Result<()>;
    /// get all tags
    async fn tags(&self) -> Result<Vec<i32>>;
    /// update `max_id` in database
    async fn update_max(&self, tag: i32) -> Result<Leaf>;
    /// update `max_id` in database by specified step
    async fn update_max_by_step(&self, tag: i32, step: i32) -> Result<Leaf>;
}
