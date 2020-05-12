// use futures_util::StreamExt;
use std::convert::{TryFrom, TryInto};

use darkredis::{Command, ConnectionPool, Value};

use async_trait::async_trait;

use crate::{Error, Leaf, LeafDao, Result};

/// Each leaf will be a hashmap with a key like `leaf_alloc:*`.
#[derive(Debug)]
pub struct RedisDao {
    pool: darkredis::ConnectionPool,
}

impl TryFrom<Value> for Leaf {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let (tag, max_id, step) = value
            .optional_array()
            .and_then(|values| {
                let mut values = values.into_iter();
                let (tag, max_id, step) = (values.next()?, values.next()?, values.next()?);
                Some((
                    tag.optional_integer().map(|v| v as u32)?,
                    max_id.optional_integer().map(|v| v as u64)?,
                    step.optional_integer().map(|v| v as u32)?,
                ))
            })
            .ok_or(Error::SerializationError)?;

        Ok(Self { tag, max_id, step })
    }
}

#[async_trait]
impl LeafDao for RedisDao {
    async fn leaves(&self) -> Result<Vec<Leaf>> {
        let mut leaves = vec![];
        for tag in self.tags().await? {
            if let Some(leaf) = self.get_leaf(tag).await {
                leaves.push(leaf);
            }
        }
        Ok(leaves)
    }

    async fn tags(&self) -> Result<Vec<u32>> {
        let mut conn = self.pool.get().await;
        let command = Command::new("KEYS").arg(b"leaf_alloc:*");
        if let Value::Array(tags) = conn.run_command(command).await? {
            Ok(tags
                .into_iter()
                .map(|v| v.optional_integer().map(|v| v as u32))
                .flatten()
                .collect::<Vec<u32>>())
        } else {
            Err(Error::SerializationError)
        }
    }

    async fn update_max(&self, tag: u32) -> Result<Leaf> {
        let step = self.get_leaf(tag).await.map(|l| l.step).unwrap_or(1000);
        self.update_max_by_step(tag, step).await
    }

    async fn update_max_by_step(&self, tag: u32, step: u32) -> Result<Leaf> {
        let mut conn = self.pool.get().await;
        conn.hincrby(format!("leaf_alloc:{}", tag), b"max_id", step as isize)
            .await?;
        self.get_leaf(tag).await.ok_or(Error::TagNotExist)
    }
}

impl RedisDao {
    pub async fn new(address: impl Into<String>, password: Option<&str>) -> Result<Self> {
        Ok(Self {
            pool: ConnectionPool::create(address.into(), password, num_cpus::get()).await?,
        })
    }

    pub async fn get_leaf(&self, tag: u32) -> Option<Leaf> {
        let mut conn = self.pool.get().await;
        let key = format!("leaf_alloc:{}", tag);
        let command = Command::new("HMGET")
            .arg(&key)
            .arg(b"tag")
            .arg(b"max_id")
            .arg(b"step");
        conn.run_command(command)
            .await
            .ok()
            .and_then(|v| v.try_into().ok())
    }
    pub async fn create_leaf(&self, tag: u32) -> Result<()> {
        let mut conn = self.pool.get().await;
        let key = format!("leaf_alloc:{}", tag).into_bytes();
        let tag_bytes = tag.to_string().into_bytes();
        let args: [&[u8]; 4] = [b"max_id", b"1000", b"step", b"1000"];
        let command = Command::new("HMSET")
            .arg(&key)
            .arg(b"tag")
            .arg(&tag_bytes)
            .args(&args);
        conn.run_command(command).await?;
        Ok(())
    }
}
