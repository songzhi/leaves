use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

// use tokio's Rwlock in the future
use async_std::sync::RwLock;
use lockfree::map::Map;

use async_trait::async_trait;
pub use error::Error;
use segment::SegmentBuffer;

pub mod segment;
pub mod error;
pub mod dao;

pub type Result<T> = std::result::Result<T, Error>;

/// data stored in DB
#[derive(Debug)]
pub struct Leaf {
    tag: u32,
    max_id: u64,
    step: u32,
}

#[async_trait]
pub trait LeafDao {
    async fn leaves(&self) -> Result<Vec<Leaf>>;
    async fn tags(&self) -> Result<Vec<u32>>;
    async fn update_max(&self, tag: u32) -> Result<Leaf>;
    async fn update_max_by_step(&self, tag: u32, step: u32) -> Result<Leaf>;
}

type Cache = Arc<Map<u32, Arc<RwLock<SegmentBuffer>>>>;

pub struct LeafSegment<D> {
    dao: Arc<D>,
    init_ok: bool,
    cache: Cache,
}

impl<D: 'static + LeafDao + Send + Sync> LeafSegment<D> {
    const MAX_STEP: u32 = 1_000_000;
    const SEGMENT_DURATION: Duration = Duration::from_secs(15 * 60);

    pub fn new(dao: Arc<D>) -> Self {
        Self {
            dao,
            init_ok: false,
            cache: Arc::new(Map::new()),
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        log::info!("Init ...");
        Self::update_cache(self.cache.clone(), self.dao.clone()).await?;
        self.init_ok = true;
        self.update_cache_every_minute();
        Ok(())
    }


    pub async fn get(&self, tag: u32) -> Result<u64> {
        if !self.init_ok {
            return Err(Error::ServiceNotReady);
        }
        let buffer = self.cache.get(&tag).ok_or_else(|| Error::TagNotExist)?.val().clone();
        if !buffer.read().await.init_ok {
            log::info!("Init Buffer[{}]", tag);
            Self::update_segment_from_db(self.dao.clone(), buffer, false, true).await?;
        }
        self.get_id_from_segment_buffer(tag).await
    }

    fn update_cache_every_minute(&self) {
        let cache = self.cache.clone();
        let dao = self.dao.clone();
        tokio::spawn(async move {
            let mut interval = tokio::timer::Interval::new_interval(Duration::from_secs(60));
            loop {
                interval.next().await;
                if Self::update_cache(cache.clone(), dao.clone()).await.is_err() {
                    log::error!("Update cache failed");
                }
            }
        });
    }

    async fn update_cache(cache: Cache, dao: Arc<D>) -> Result<()> {
        log::info!("Update cache");
        let db_tags = dao.tags().await?;
        if db_tags.is_empty() { return Ok(()); }
        let cache_tags = cache.iter().map(|e| *e.key());
        let insert_tags = db_tags.iter().copied().filter(|t| cache.get(t).is_none()).collect::<Vec<_>>();
        let remove_tags = cache_tags.filter(|t| !db_tags.contains(t)).collect::<Vec<_>>();
        for t in insert_tags {
            log::info!("Add tag[{}] to cache",t);
            cache.insert(t, Arc::new(RwLock::new(SegmentBuffer::new(t))));
        }
        for t in remove_tags {
            log::info!("Remove tag[{}] from cache", t);
            cache.remove(&t);
        }
        Ok(())
    }

    pub async fn get_id_from_segment_buffer(&self, tag: u32) -> Result<u64> {
        let buffer_wrapped = self.cache.get(&tag).ok_or_else(|| Error::TagNotExist)?.val().clone();
        let buffer = buffer_wrapped.read().await;
        let segment = buffer.current();
        if !buffer.next_ready && (segment.idle() < (segment.step * 9 / 10) as u64) &&
            !buffer.thread_running.compare_and_swap(false, true, Ordering::Relaxed) {
            let dao = self.dao.clone();
            let buffer_wrapped = buffer_wrapped.clone();
            tokio::spawn(async move {
                if Self::update_segment_from_db(dao, buffer_wrapped.clone(), true, false).await.is_ok() {
                    log::info!("Update Buffer[{}]'s next segment from DB", tag);
                    let mut buffer = buffer_wrapped.write().await;
                    buffer.next_ready = true;
                }
                let buffer = buffer_wrapped.read().await;
                buffer.thread_running.store(false, Ordering::Relaxed);
            });
        }
        let val = segment.val.fetch_add(1, Ordering::Relaxed);
        if val < segment.max {
            Ok(val)
        } else {
            drop(buffer); // to prevent deadlock
            self.wait_and_sleep(tag).await?;
            let mut buffer = buffer_wrapped.write().await;
            let segment = buffer.current();
            let val = segment.val.fetch_add(1, Ordering::Relaxed);
            if val < segment.max {
                Ok(val)
            } else if buffer.next_ready {
                log::info!("Buffer[{}] switched", tag);
                buffer.switch();
                buffer.next_ready = false;
                Ok(buffer.current().val.fetch_add(1, Ordering::Relaxed))
            } else {
                Err(Error::BothSegmentsNotReady)
            }
        }
    }

    pub async fn update_segment_from_db(dao: Arc<D>, buffer: Arc<RwLock<SegmentBuffer>>, is_next: bool, is_init: bool) -> Result<()> {
        let mut buffer = buffer.write().await;
        if is_init && buffer.init_ok {
            return Ok(());
        }
        let leaf = if !buffer.init_ok {
            let leaf = dao.update_max(buffer.tag).await?;
            buffer.step = leaf.step;
            buffer.min_step = leaf.step;
            buffer.init_ok = true;
            leaf
        } else if buffer.update_timestamp == 0 {
            let leaf = dao.update_max(buffer.tag).await?;
            buffer.update_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
            buffer.step = leaf.step;
            buffer.min_step = leaf.step;
            leaf
        } else {
            let duration = SystemTime::now().duration_since(
                SystemTime::UNIX_EPOCH + Duration::from_millis(buffer.update_timestamp as u64)).unwrap();
            let step = buffer.step;
            let next_step =
                if duration < Self::SEGMENT_DURATION && step * 2 <= Self::MAX_STEP {
                    step * 2
                } else if duration >= Self::SEGMENT_DURATION * 2 && step / 2 >= buffer.min_step {
                    step / 2
                } else { step };
            log::info!("Buffer[{}] step:{} duration:{:.2}mins next_step:{}",
                buffer.tag, step, duration.as_secs() as f64 / 60.0, next_step);
            let leaf = dao.update_max_by_step(buffer.tag, next_step).await?;
            buffer.update_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
            buffer.step = next_step;
            buffer.min_step = leaf.step;
            leaf
        };
        let step = buffer.step;
        let segment = if is_next { buffer.next_mut() } else { buffer.current_mut() };
        segment.val.store(leaf.max_id - step as u64, Ordering::Relaxed);
        segment.max = leaf.max_id;
        segment.step = step;
        Ok(())
    }

    async fn wait_and_sleep(&self, tag: u32) -> Result<()> {
        let mut roll = 0;
        let buffer = self.cache.get(&tag).ok_or(Error::TagNotExist)?.val().clone();
        let buffer = buffer.read().await;
        while buffer.thread_running.load(Ordering::Relaxed) {
            roll += 1;
            if roll > 10_000 {
                tokio::timer::delay_for(Duration::from_millis(10)).await;
                break;
            }
        }
        Ok(())
    }
}