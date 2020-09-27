use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use leaves::dao::mock::MockLeafDao;

use leaves::segment::Config;
use leaves::{Leaf, LeafDao, SegmentIDGen};

fn bench_mock(c: &mut Criterion) {
    let dao = Arc::new(MockLeafDao::default());
    let id = "Segment[Mock]";
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let tag = fastrand::i32(1..100000);
    let service = rt.block_on(async {
        dao.insert(Leaf {
            tag,
            max_id: 0,
            step: 1000,
        })
        .await
        .unwrap();
        let mut service = SegmentIDGen::new(dao, Config::new());
        service.init().await.unwrap();
        service
    });
    c.bench_function(id, |b| {
        b.iter(|| {
            rt.block_on(async {
                service.get(tag).await.unwrap();
            });
        })
    });
}

criterion_group!(benches, bench_mock);
criterion_main!(benches);
