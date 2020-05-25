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
        fn redis_value_to_isize(v: Value) -> Option<isize> {
            match v {
                Value::String(s) => String::from_utf8(s).ok()?.parse::<isize>().ok(),
                Value::Integer(i) => Some(i),
                _ => None,
            }
        }
        let (tag, max_id, step) = value
            .optional_array()
            .and_then(|values| {
                let mut values = values.into_iter();
                let (tag, max_id, step) = (values.next()?, values.next()?, values.next()?);
                Some((
                    redis_value_to_isize(tag)? as i32,
                    redis_value_to_isize(max_id)? as i64,
                    redis_value_to_isize(step)? as i32,
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
            if let Ok(leaf) = self.leaf(tag).await {
                leaves.push(leaf);
            }
        }
        Ok(leaves)
    }

    async fn leaf(&self, tag: i32) -> Result<Leaf> {
        let mut conn = self.pool.get().await;
        let key = format!("leaf_alloc:{}", tag);
        let command = Command::new("HMGET")
            .arg(&key)
            .arg(b"tag")
            .arg(b"max_id")
            .arg(b"step");
        conn.run_command(command).await?.try_into()
    }

    async fn insert(&self, leaf: Leaf) -> Result<()> {
        let mut conn = self.pool.get().await;
        let key = format!("leaf_alloc:{}", leaf.tag).into_bytes();
        let tag_bytes = leaf.tag.to_string().into_bytes();
        let max_id_bytes = leaf.max_id.to_string().into_bytes();
        let step_bytes = leaf.step.to_string().into_bytes();
        let command = Command::new("HMSET")
            .arg(&key)
            .arg(b"tag")
            .arg(&tag_bytes)
            .arg(b"max_id")
            .arg(&max_id_bytes)
            .arg(b"step")
            .arg(&step_bytes);
        conn.run_command(command).await?;
        Ok(())
    }

    async fn tags(&self) -> Result<Vec<i32>> {
        let mut conn = self.pool.get().await;
        let command = Command::new("KEYS").arg(b"leaf_alloc:*");
        Ok(conn
            .run_command(command)
            .await?
            .optional_array()
            .map(|tags| {
                tags.into_iter()
                    .map(|v| {
                        v.optional_string()
                            .and_then(|v| {
                                String::from_utf8(v.rsplit(|i| i.eq(&b':')).next()?.into()).ok()
                            })
                            .and_then(|s| s.parse::<i32>().ok())
                    })
                    .flatten()
                    .collect()
            })
            .unwrap_or(vec![]))
    }

    async fn update_max(&self, tag: i32) -> Result<Leaf> {
        let step = self.leaf(tag).await.map(|l| l.step).unwrap_or(1000);
        self.update_max_by_step(tag, step).await
    }

    async fn update_max_by_step(&self, tag: i32, step: i32) -> Result<Leaf> {
        let mut conn = self.pool.get().await;
        conn.hincrby(format!("leaf_alloc:{}", tag), b"max_id", step as isize)
            .await?;
        self.leaf(tag).await
    }
}

impl RedisDao {
    pub async fn new(address: impl Into<String>, password: Option<&str>) -> Result<Self> {
        Ok(Self {
            pool: ConnectionPool::create(address.into(), password, num_cpus::get()).await?,
        })
    }
}
