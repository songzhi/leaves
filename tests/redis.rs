use std::sync::Arc;

use leaves::{Leaf, LeafDao, LeafSegment};

#[tokio::test]
async fn test_with_redis() {
    dotenv::dotenv().ok();
    let mut config = std::env::var("REDIS_URL").expect("REDIS_URL").split(' ');
    let (address, password) = (config.next().unwrap(), config.next());
    let dao = Arc::new(
        leaves::dao::redis::RedisDao::new(address, password)
            .await
            .unwrap(),
    );
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
