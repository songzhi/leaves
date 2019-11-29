use std::fmt::Display;
use std::net::SocketAddr;

use redis_async::client::{paired_connect, PairedConnection};
use redis_async::resp_array;

use async_trait::async_trait;

use crate::{Error, Leaf, LeafDao, Result};

/// Each leaf will be a hashmap with a key like `leaf_alloc:*`.
#[derive(Debug)]
pub struct RedisDao {
    conn: PairedConnection,
}

#[async_trait]
impl LeafDao for RedisDao {
    async fn leaves(&self) -> Result<Vec<Leaf>> {
        let tags: Vec<String> = self.conn.send(resp_array!("KEYS", "leaf_alloc:*")).await?;
        let tasks = tags.iter().map(|t| tokio::task::spawn(self.get_leaf(t)));
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
            .map(|t| t.parse())
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
            .send::<()>(resp_array!(
                "HINCRBY",
                format!("leaf_alloc:{}", tag),
                format!("{}", step)
            ))
            .await?;
        self.get_leaf(tag).await.ok_or(Error::TagNotExist)
    }
}

impl RedisDao {
    pub async fn new(addr: &str) -> Self {
        let addr = addr.parse().unwrap();
        Self {
            conn: paired_connect(&addr).await.unwrap(),
        }
    }

    pub async fn auth(&self, password: &str) -> Result<()> {
        self.conn.send(resp_array!("AUTH", password)).await.into()
    }

    pub async fn select_db(&self, db: &str) -> Result<()> {
        self.conn.send(resp_array!("SELECT", db)).await.into()
    }

    async fn get_leaf<T: Display>(&self, tag: T) -> Option<Leaf> {
        let data = self
            .conn
            .send::<Vec<String>>(resp_array!(
                "HMGET",
                format!("leaf_alloc:{}", tag),
                "tag",
                "max_id",
                "step"
            ))
            .await?;
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
}

impl RedisDao {
    pub async fn new(addr: &SocketAddr) -> Result<Self> {
        Ok(Self {
            conn: paired_connect(addr).await?,
        })
    }
}
