use std::sync::Arc;

use mongodb::{Client, options::ClientOptions};

use leaves::{Leaf, LeafDao, LeafSegment};

#[tokio::test]
async fn test_with_mongodb() {
    dotenv::dotenv().ok();
    let url = std::env::var("MONGODB_URL").expect("MONGODB_URL");
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
