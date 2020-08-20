use std::sync::Arc;

use mongodb::{options::ClientOptions, Client};

use leaves::segment::Config;
use leaves::{Leaf, LeafDao, SegmentIDGen};
use std::time::Instant;

#[tokio::test]
async fn test_with_mongodb() {
    let start = Instant::now();
    dotenv::dotenv().ok();
    let url = std::env::var("MONGODB_URL").expect("MONGODB_URL");
    let client_options = ClientOptions::parse(url.as_str()).await.unwrap();
    let client = Client::with_options(client_options).unwrap();
    let collection = client.database("test_leaves").collection("leaves");
    let dao = Arc::new(leaves::dao::MongoLeafDao::new(collection));
    let tag = fastrand::i32(1..1_000_000);
    dao.insert(Leaf {
        tag,
        max_id: 0,
        step: 1000,
    })
        .await
        .unwrap();
    let mut service = SegmentIDGen::new(dao.clone(), Config::new());
    service.init().await.unwrap();
    for _ in 0..100_0000 {
        service.get(tag).await.unwrap();
    }
    println!("{}ms", start.elapsed().as_millis());
}
