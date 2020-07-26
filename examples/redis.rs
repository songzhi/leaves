use std::sync::Arc;

use leaves::dao::redis::RedisDao;
use leaves::{SegmentIDGen, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let dao = Arc::new(RedisDao::new("127.0.0.1:6379", None).await?);
    let tag = 251;
    // dao.create_leaf(tag).await?;
    let mut service = SegmentIDGen::new(dao);
    service.init().await?;
    println!(
        "{:?}",
        service
            .cache
            .iter()
            .map(|x| x.key().clone())
            .collect::<Vec<_>>()
    );
    for _ in 0..1000 {
        let i = service.get(tag).await?;
        println!("{:?}", i);
    }
    Ok(())
}
