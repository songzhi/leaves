use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use leaves::dao::mock::MockLeafDao;

use leaves::segment::Config;
use leaves::{Leaf, LeafDao, SegmentIDGen};

async fn mock(service: Arc<SegmentIDGen<MockLeafDao>>, tags: i32) {
    let tasks = (1..=tags)
        .cycle()
        .take(5)
        .map(|tag| {
            let service = service.clone();
            tokio::spawn(async move {
                for _ in 0..10000 {
                    service.get(tag).await.ok();
                }
            })
        })
        .collect::<Vec<_>>();
    for t in tasks {
        t.await.ok();
    }
}

fn bench_mock(c: &mut Criterion) {
    let dao = Arc::new(MockLeafDao::default());
    let id = "Segment[Mock]";
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let tags = 5;
    let service = rt.block_on(async {
        for tag in 1..=tags {
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
        service
    });
    let service = Arc::new(service);
    c.bench_function(id, |b| {
        b.iter(|| {
            rt.block_on(async {
                mock(service.clone(), tags).await;
            });
        })
    });
}

criterion_group!(benches, bench_mock);
criterion_main!(benches);
