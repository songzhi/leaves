use mongodb::Collection;

use async_trait::async_trait;

use crate::{Leaf, LeafDao, Result};

pub struct MongoLeafDao {
    collection: Collection,
}
