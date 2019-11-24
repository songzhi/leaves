# Leaves: Distributed ID Generation Service

This is a unofficial port for [Leaf](https://github.com/Meituan-Dianping/Leaf).

### 🏠 [Homepage](https://github.com/songzhi/leaves)

## Features
*[x] generate id in segment mode
*[ ] generate id in snowflake mode 
*[x] using mysql 
*[ ] using redis
*[ ] http server or rpc service(actually just implement it by yourself 😂)

## TODO
* more configurable
* using tokio's Rwlock in the future.

## Example
```rust
use leaves::dao::mysql::MySqlLeafDao;
use leaves::LeafSegment;
use leaves::Result;

#[tokio::main]
async main() -> Result<()> {
    let dao = Arc::new(MySqlLeafDao::new("mysql://...")?);
    let mut service = LeafSegment::new(dao);
    service.init().await?;
    let tag = 1;
    for _ in 0..1000 {
        println!("{}", service.get(tag).await?);
    }
}
```

## Benchmark
Not yet.But in my PC and database locating on remote server,I used 10 threads and each one loop 1000 times.
It costs 800μs.