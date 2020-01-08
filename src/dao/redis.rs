use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Arc;

use redis_async::client::{paired_connect, PairedConnection};
use redis_async::resp_array;

use async_trait::async_trait;

use crate::{Error, Leaf, LeafDao, Result};

/// Each leaf will be a hashmap with a key like `leaf_alloc:*`.
#[derive(Debug)]
pub struct RedisDao {
    conn: Arc<PairedConnection>,
}

#[async_trait]
impl LeafDao for RedisDao {
    async fn leaves(&self) -> Result<Vec<Leaf>> {
        let tags: Vec<String> = self.conn.send(resp_array!("KEYS", "leaf_alloc:*")).await?;
        let conn = self.conn.clone();
        let tasks = tags
            .into_iter()
            .map(|t| tokio::task::spawn(Self::get_leaf(conn.clone(), t)));
        let mut leaves = vec![];
        for task in tasks {
            if let Ok(Some(leaf)) = task.await {
                leaves.push(leaf);
            }
        }
        Ok(leaves)
    }

    async fn tags(&self) -> Result<Vec<u32>> {
        Ok(self
            .conn
            .send::<Vec<String>>(resp_array!("KEYS", "leaf_alloc:*"))
            .await?
            .into_iter()
            .map(|t| t["leaf_alloc:".len()..].parse())
            .flatten()
            .collect())
    }

    async fn update_max(&self, tag: u32) -> Result<Leaf> {
        let step: u32 = self
            .conn
            .send::<String>(resp_array!("HGET", format!("leaf_alloc:{}", tag), "step"))
            .await?
            .parse()
            .or(Err(Error::TagNotExist))?;
        self.update_max_by_step(tag, step).await
    }

    async fn update_max_by_step(&self, tag: u32, step: u32) -> Result<Leaf> {
        self.conn
            .send::<String>(resp_array!(
                "HINCRBY",
                format!("leaf_alloc:{}", tag),
                "max_id",
                format!("{}", step)
            ))
            .await?;
        Self::get_leaf(self.conn.clone(), tag)
            .await
            .ok_or(Error::TagNotExist)
    }
}

impl RedisDao {
    pub async fn new(addr: &SocketAddr) -> Result<Self> {
        Ok(Self {
            conn: Arc::new(paired_connect(addr).await?),
        })
    }

    pub async fn auth(&self, password: &str) -> Result<()> {
        self.conn.send(resp_array!("AUTH", password)).await?;
        Ok(())
    }

    pub async fn select_db(&self, db: &str) -> Result<()> {
        self.conn.send(resp_array!("SELECT", db)).await?;
        Ok(())
    }

    pub async fn get_leaf<T: Display>(conn: Arc<PairedConnection>, tag: T) -> Option<Leaf> {
        let data = conn
            .send::<Vec<String>>(resp_array!(
                "HMGET",
                format!("leaf_alloc:{}", tag),
                "tag",
                "max_id",
                "step"
            ))
            .await
            .ok()?;
        if data.len() != 3 {
            None
        } else {
            Some(Leaf {
                tag: data[0].parse().ok()?,
                max_id: data[1].parse().ok()?,
                step: data[2].parse().ok()?,
            })
        }
    }
    pub async fn create_leaf<T: Display>(&self, tag: T) -> Result<()> {
        self.conn
            .send::<()>(resp_array!(
                "HMSET",
                format!("leaf_alloc:{}", tag),
                "tag",
                format!("{}", tag),
                "max_id",
                "1000",
                "step",
                "1000"
            ))
            .await?;
        Ok(())
    }
}
