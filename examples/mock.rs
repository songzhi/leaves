use std::sync::Arc;
use std::time::Instant;

use leaves::{Leaf, LeafDao, LeafSegment};
use leaves::dao::mock::MockLeafDao;

#[tokio::main]
async fn main() {
    let start = Instant::now();
    let dao = Arc::new(MockLeafDao::default());
    let mut service = LeafSegment::new(dao.clone());
    dao.insert(Leaf {
        tag: 1,
        max_id: 0,
        step: 1000,
    })
        .await
        .unwrap();
    service.init().await.unwrap();
    for _ in 0..100_0000 {
        service.get(1).await.unwrap();
    }
    println!("Finished in {}ms", start.elapsed().as_millis());
}
