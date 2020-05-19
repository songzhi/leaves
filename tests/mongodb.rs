use std::sync::Arc;

use mongodb::{options::ClientOptions, Client};

use leaves::{Leaf, LeafDao, LeafSegment};

include!("./utils.rs");

#[tokio::test]
async fn test_with_mongodb() {
    let url = get_config("tests/config.toml").unwrap().mongodb.unwrap();
    let client_options = ClientOptions::parse(url.as_str()).await.unwrap();
    let client = Client::with_options(client_options).unwrap();
    let collection = client.database("test_leaves").collection("leaves");
    let dao = Arc::new(leaves::dao::mongodb::MongoLeafDao::new(collection));
    let mut service = LeafSegment::new(dao.clone());
    service.init().await.unwrap();
    // dao.insert(Leaf {
    //     tag: 1,
    //     max_id: 0,
    //     step: 1000,
    // })
    // .await
    // .unwrap();
    for _ in 0..10000 {
        service.get(1).await.unwrap();
    }
}
