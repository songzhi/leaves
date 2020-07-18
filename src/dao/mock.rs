use async_trait::async_trait;
use dashmap::DashMap;

use crate::{Error, Leaf, Result};

use super::LeafDao;

#[derive(Debug, Default)]
pub struct MockLeafDao {
    leaves: DashMap<i32, Leaf>,
}

#[async_trait]
impl LeafDao for MockLeafDao {
    async fn leaves(&self) -> Result<Vec<Leaf>> {
        Ok(self.leaves.iter().map(|r| *r.value()).collect())
    }

    async fn leaf(&self, tag: i32) -> Result<Leaf> {
        self.leaves
            .get(&tag)
            .map(|r| *r.value())
            .ok_or(Error::TagNotExist)
    }

    async fn insert(&self, leaf: Leaf) -> Result<()> {
        self.leaves.insert(leaf.tag, leaf);
        Ok(())
    }

    async fn tags(&self) -> Result<Vec<i32>> {
        Ok(self.leaves.iter().map(|r| *r.key()).collect())
    }

    async fn update_max(&self, tag: i32) -> Result<Leaf> {
        self.leaves
            .update_get(&tag, |_, leaf| {
                let max_id = leaf.max_id + leaf.step as i64;
                Leaf { max_id, ..*leaf }
            })
            .ok_or(Error::TagNotExist)
            .map(|elem| *elem.value())
    }

    async fn update_max_by_step(&self, tag: i32, step: i32) -> Result<Leaf> {
        self.leaves
            .update_get(&tag, |_, leaf| {
                let max_id = leaf.max_id + step as i64;
                Leaf { max_id, ..*leaf }
            })
            .ok_or(Error::TagNotExist)
            .map(|elem| *elem.value())
    }
}
