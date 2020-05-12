use sqlx::row::Row;
use sqlx::sqlite::{SqlitePool, SqliteQueryAs};

use async_trait::async_trait;

use crate::{Leaf, LeafDao, Result};

pub struct PgLeafDao {
    pool: SqlitePool,
}

#[async_trait]
impl LeafDao for PgLeafDao {
    async fn leaves(&self) -> Result<Vec<Leaf>> {
        let mut conn = self.pool.acquire().await?;
        let leaves: Vec<Leaf> = sqlx::query_as("SELECT tag, max_id, step FROM leaf_alloc")
            .fetch_all(&mut conn)
            .await?;
        Ok(leaves)
    }

    async fn tags(&self) -> Result<Vec<u32>> {
        let mut conn = self.pool.acquire().await?;
        let rows: Vec<(u32,)> = sqlx::query_as("SELECT tag FROM leaf_alloc")
            .fetch_all(&mut conn)
            .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    async fn update_max(&self, tag: u32) -> Result<Leaf> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("UPDATE leaf_alloc SET max_id = max_id + step WHERE tag = ?")
            .bind(tag)
            .execute(&mut conn)
            .await?;
        self.get_leaf(tag).await
    }

    async fn update_max_by_step(&self, tag: u32, step: u32) -> Result<Leaf> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("UPDATE leaf_alloc SET max_id = max_id + ? WHERE tag = ?")
            .bind(step)
            .bind(tag)
            .execute(&mut conn)
            .await?;
        self.get_leaf(tag).await
    }
}

impl PgLeafDao {
    pub async fn new(db_url: &str) -> Result<Self> {
        Ok(Self {
            pool: SqlitePool::new(db_url).await?,
        })
    }

    async fn get_leaf(&self, tag: u32) -> Result<Leaf> {
        let mut conn = self.pool.acquire().await?;
        let leaf: Leaf = sqlx::query("SELECT tag, max_id, step FROM leaf_alloc WHERE tag = ?")
            .bind(tag)
            .fetch_one(&mut conn)
            .await?;
        Ok(leaf)
    }

    pub async fn create_table(&self) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query(
            r#"CREATE TABLE leaf_alloc (
                    tag INT PRIMARY KEY,
                    max_id INT NOT NULL,
                    step INT NOT NULL
                )"#,
        )
        .execute(&mut conn)
        .await?;
        Ok(())
    }
    pub async fn insert_row(&self, tag: u32) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("INSERT INTO leaf_alloc (tag, max_id, step) VALUES (?, 1000, 1000)")
            .bind(tag)
            .execute(&mut conn)
            .await?;
        Ok(())
    }
}
