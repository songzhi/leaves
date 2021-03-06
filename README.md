# Leaves: Distributed ID Generation Service

This is a unofficial port of [Leaf](https://github.com/Meituan-Dianping/Leaf).

🏠 [Homepage](https://github.com/songzhi/leaves)

[![Latest version](https://img.shields.io/crates/v/leaves.svg)](https://crates.io/crates/leaves)

## Features
- [x] generate id in segment mode
- [ ] generate id in snowflake mode 
- [x] mysql 
- [x] redis
- [x] postgresql
- [x] sqlite
- [x] mongodb
- [x] runtime-agnostic(tokio or async-std) when using mysql or postgres
- [x] lazy mode: fetch leaf by tag lazily and needs remove it manually
- [ ] http server or rpc service(actually just implement it by yourself 😂)

## TODO
* performance
* correctness

## Example
Enabling the `mysql` and `runtime-tokio` feature:
```rust
use leaves::dao::MySqlLeafDao;
use leaves::{SegmentIDGen, Config, Result};

#[tokio::main]
async main() -> Result<()> {
    let dao = Arc::new(MySqlLeafDao::new("mysql://...").await?);
    let mut service = SegmentIDGen::new(dao, Config::new());
    service.init().await?;
    let tag = 1;
    for _ in 0..1000 {
        println!("{}", service.get(tag).await?);
    }
}
```

## Benchmark
1,000,000 IDs in 15ms(local MongoDB with R7 3700X)
