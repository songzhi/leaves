use mysql_async::prelude::Queryable;

use async_trait::async_trait;

use crate::{Error, Leaf, LeafDao, Result};

pub struct MySqlLeafDao {
    pool: mysql_async::Pool,
}

#[async_trait]
impl LeafDao for MySqlLeafDao {
    async fn leaves(&self) -> Result<Vec<Leaf>> {
        let conn = self.pool.get_conn().await?;
        let result = conn
            .prep_exec("SELECT tag, max_id, step FROM leaf_alloc", ())
            .await?;
        let (_ /* conn */, leaves) = result
            .map_and_drop(|row| {
                let (tag, max_id, step) = mysql_async::from_row(row);
                Leaf { tag, max_id, step }
            })
            .await?;
        Ok(leaves)
    }

    async fn tags(&self) -> Result<Vec<u32>> {
        let conn = self.pool.get_conn().await?;
        let result = conn.prep_exec("SELECT tag from leaf_alloc", ()).await?;
        let (_, tags) = result
            .map_and_drop(|row| {
                let (tag, ) = mysql_async::from_row(row);
                tag
            })
            .await?;
        Ok(tags)
    }

    async fn update_max(&self, tag: u32) -> Result<Leaf> {
        let conn = self.pool.get_conn().await?;
        conn.drop_exec(
            "UPDATE leaf_alloc SET max_id = max_id + step WHERE tag = :tag",
            (tag, ),
        )
            .await?;
        self.get_leaf(tag).await
    }

    async fn update_max_by_step(&self, tag: u32, step: u32) -> Result<Leaf> {
        let conn = self.pool.get_conn().await?;
        conn.drop_exec(
            "UPDATE leaf_alloc SET max_id = max_id + :step WHERE tag = :tag",
            (step, tag),
        )
            .await?;
        self.get_leaf(tag).await
    }
}

impl MySqlLeafDao {
    pub fn new(db_url: &str) -> Result<Self> {
        Ok(Self {
            pool: mysql_async::Pool::from_url(db_url)?,
        })
    }
    async fn get_leaf(&self, tag: u32) -> Result<Leaf> {
        let conn = self.pool.get_conn().await?;
        let result = conn
            .prep_exec(
                "SELECT tag, max_id, step FROM leaf_alloc WHERE tag = :tag",
                (tag, ),
            )
            .await?;
        let (_, leaf) = result
            .map_and_drop(|row| {
                let (tag, max_id, step) = mysql_async::from_row(row);
                Leaf { tag, max_id, step }
            })
            .await?;
        leaf.into_iter().next().ok_or(Error::TagNotExist)
    }
    pub async fn create_table(&self) -> Result<()> {
        let conn = self.pool.get_conn().await?;
        conn.drop_query(
            r"CREATE TABLE leaf_alloc (
            tag int not null,
            max_id int not null,
            step int not null
        )",
        )
            .await?;
        Ok(())
    }
    pub async fn insert_row(&self, tag: u32) -> Result<()> {
        let conn = self.pool.get_conn().await?;
        conn.prep_exec(
            "INSERT INTO leaf_alloc (tag, max_id, step) VALUES (:tag, 1000, 1000)",
            (tag, ),
        )
            .await?;
        Ok(())
    }
}
