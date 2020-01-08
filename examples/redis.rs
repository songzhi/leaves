use std::net::SocketAddr;
use std::sync::Arc;

use leaves::dao::redis::RedisDao;
use leaves::{LeafSegment, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let dao = Arc::new(RedisDao::new(&addr).await?);
    dao.auth("password").await?;
    dao.select_db("2").await?;
    let tag = 251;
    //        dao.create_leaf(tag).await?;
    let mut service = LeafSegment::new(dao);
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
