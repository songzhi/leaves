use std::sync::Arc;

use mongodb::{options::ClientOptions, Client};

use leaves::segment::Config;
use leaves::{Leaf, LeafDao, SegmentIDGen};
use std::time::Instant;

#[tokio::test]
async fn test_with_mongodb() {
    dotenv::dotenv().ok();
    let url = std::env::var("MONGODB_URL").expect("MONGODB_URL");
    let client_options = ClientOptions::parse(url.as_str()).await.unwrap();
    let client = Client::with_options(client_options).unwrap();
    let collection = client.database("test_leaves").collection("leaves");
    let dao = Arc::new(leaves::dao::MongoLeafDao::new(collection));
    let tags = (0..5)
        .map(|_| fastrand::i32(1..1_000_000))
        .collect::<Vec<_>>();
    for &tag in tags.iter() {
        dao.insert(Leaf {
            tag,
            max_id: 0,
            step: 1000,
        })
        .await
        .unwrap();
    }
    let mut service = SegmentIDGen::new(dao, Config::new());
    service.init().await.unwrap();
    let service = Arc::new(service);
    let start = Instant::now();
    let tasks = tags
        .into_iter()
        .cycle()
        .take(5)
        .map(|tag| {
            let service = service.clone();
            tokio::spawn(async move {
                let mut guard = service.get_tag_guard(tag).await.unwrap();
                for _ in 0..40000 {
                    guard.get().await.ok();
                }
            })
        })
        .collect::<Vec<_>>();
    for t in tasks {
        t.await.ok();
    }
    println!("{}ms", start.elapsed().as_millis());
}
