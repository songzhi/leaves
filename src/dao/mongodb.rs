use futures_util::StreamExt;
use mongodb::Collection;

use async_trait::async_trait;

use crate::{Error, Leaf, LeafDao, Result};

pub struct MongoLeafDao {
    collection: Collection,
}

#[async_trait]
impl LeafDao for MongoLeafDao {
    async fn leaves(&self) -> Result<Vec<Leaf>> {
        Ok(self
            .collection
            .find(None, None)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(mongodb::error::Result::ok)
            .filter_map(|doc| bson::from_bson(doc.into()).ok())
            .collect())
    }

    async fn leaf(&self, tag: i32) -> Result<Leaf> {
        let filter = bson::doc! {
          "tag": tag
        };
        self.collection
            .find_one(filter, None)
            .await?
            .and_then(|doc| bson::from_bson(doc.into()).ok())
            .ok_or(Error::TagNotExist)
    }

    async fn insert(&self, leaf: Leaf) -> Result<()> {
        let doc = bson::to_bson(&leaf)?
            .as_document()
            .ok_or(Error::SerializationError)?
            .clone();
        self.collection.insert_one(doc, None).await?;
        Ok(())
    }

    async fn tags(&self) -> Result<Vec<i32>> {
        let projection = bson::doc! {
            "tag" :1
        };
        let options = mongodb::options::FindOptions::builder()
            .projection(projection)
            .build();
        Ok(self
            .collection
            .find(None, options)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(mongodb::error::Result::ok)
            .filter_map(|doc| doc.get_i32("tag").ok())
            .collect())
    }

    async fn update_max(&self, tag: i32) -> Result<Leaf> {
        // FIXME: rewrite
        self.update_max_by_step(tag, 1000).await
    }

    async fn update_max_by_step(&self, tag: i32, step: i32) -> Result<Leaf> {
        let filter = bson::doc! {
            "tag": tag
        };
        let update = bson::doc! {
            "$inc": {
                "max_id": step
            }
        };
        let options = mongodb::options::FindOneAndUpdateOptions::builder()
            .return_document(mongodb::options::ReturnDocument::After)
            .build();
        self.collection
            .find_one_and_update(filter, update, options)
            .await?
            .and_then(|doc| bson::from_bson(doc.into()).ok())
            .ok_or(Error::TagNotExist)
    }
}

impl MongoLeafDao {
    pub fn new(collection: Collection) -> Self {
        Self { collection }
    }
}
