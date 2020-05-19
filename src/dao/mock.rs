use dashmap::DashMap;

use async_trait::async_trait;

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
        if let Some(mut leaf) = self.leaves.get_mut(&tag) {
            leaf.max_id += leaf.step as i64;
            Ok(*leaf.value())
        } else {
            Err(Error::TagNotExist)
        }
    }

    async fn update_max_by_step(&self, tag: i32, step: i32) -> Result<Leaf> {
        if let Some(mut leaf) = self.leaves.get_mut(&tag) {
            leaf.max_id += step as i64;
            Ok(*leaf.value())
        } else {
            Err(Error::TagNotExist)
        }
    }
}
