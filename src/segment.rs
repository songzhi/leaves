use std::cmp::min;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[cfg(feature = "runtime-async-std")]
use async_std::{sync::RwLock, task::spawn};
use dashmap::DashMap;
#[cfg(feature = "runtime-tokio")]
use tokio::{sync::RwLock, task::spawn};

use crate::{Error, LeafDao, Result};

type Cache = Arc<DashMap<i32, Arc<RwLock<SegmentBuffer>>>>;

pub struct LeafSegment<D> {
    dao: Arc<D>,
    init_ok: bool,
    pub cache: Cache,
}

impl<D: 'static + LeafDao + Send + Sync> LeafSegment<D> {
    const MAX_STEP: i32 = 1_000_000;
    const SEGMENT_DURATION: Duration = Duration::from_secs(15 * 60);

    pub fn new(dao: Arc<D>) -> Self {
        Self {
            dao,
            init_ok: false,
            cache: Arc::new(DashMap::new()),
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        tracing::info!("Init ...");
        Self::update_cache(self.cache.clone(), self.dao.clone()).await?;
        self.init_ok = true;
        self.update_cache_every_minute();
        Ok(())
    }

    pub async fn get(&self, tag: i32) -> Result<i64> {
        if !self.init_ok {
            return Err(Error::ServiceNotReady);
        }
        let buffer = self
            .cache
            .get(&tag)
            .ok_or_else(|| Error::TagNotExist)?
            .clone();
        if !buffer.read().await.init_ok {
            tracing::info!("Init Buffer[{}]", tag);
            Self::update_segment_from_db(self.dao.clone(), buffer, false, true).await?;
        }
        self.get_id_from_segment_buffer(tag).await
    }

    fn update_cache_every_minute(&self) {
        let cache = self.cache.clone();
        let dao = self.dao.clone();
        spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if Self::update_cache(cache.clone(), dao.clone())
                    .await
                    .is_err()
                {
                    tracing::error!("Update cache failed");
                }
            }
        });
    }

    async fn update_cache(cache: Cache, dao: Arc<D>) -> Result<()> {
        tracing::info!("Update cache");
        let db_tags = dao.tags().await?;
        if db_tags.is_empty() {
            return Ok(());
        }
        let cache_tags = cache.iter().map(|e| *e.key());
        let insert_tags = db_tags
            .iter()
            .copied()
            .filter(|t| cache.get(t).is_none())
            .collect::<Vec<_>>();
        let remove_tags = cache_tags
            .filter(|t| !db_tags.contains(t))
            .collect::<Vec<_>>();
        for t in insert_tags {
            tracing::info!("Add tag[{}] to cache", t);
            cache.insert(t, Arc::new(RwLock::new(SegmentBuffer::new(t))));
        }
        for t in remove_tags {
            tracing::info!("Remove tag[{}] from cache", t);
            cache.remove(&t);
        }
        Ok(())
    }

    pub async fn get_id_from_segment_buffer(&self, tag: i32) -> Result<i64> {
        let buffer_wrapped = self
            .cache
            .get(&tag)
            .ok_or_else(|| Error::TagNotExist)?
            .clone();
        let buffer = buffer_wrapped.read().await;
        let segment = buffer.current();
        if !buffer.next_ready
            && (segment.idle() < (segment.step * 9 / 10) as i64)
            && !buffer
                .thread_running
                .compare_and_swap(false, true, Ordering::Relaxed)
        {
            let dao = self.dao.clone();
            let buffer_wrapped = buffer_wrapped.clone();
            spawn(async move {
                if Self::update_segment_from_db(dao, buffer_wrapped.clone(), true, false)
                    .await
                    .is_ok()
                {
                    tracing::info!("Update Buffer[{}]'s next segment from DB", tag);
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
                tracing::info!("Buffer[{}] switched", tag);
                buffer.switch();
                buffer.next_ready = false;
                Ok(buffer.current().val.fetch_add(1, Ordering::Relaxed))
            } else {
                Err(Error::BothSegmentsNotReady)
            }
        }
    }

    pub async fn update_segment_from_db(
        dao: Arc<D>,
        buffer: Arc<RwLock<SegmentBuffer>>,
        is_next: bool,
        is_init: bool,
    ) -> Result<()> {
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
            buffer.update_timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            buffer.step = leaf.step;
            buffer.min_step = leaf.step;
            leaf
        } else {
            let duration = SystemTime::now()
                .duration_since(
                    SystemTime::UNIX_EPOCH + Duration::from_millis(buffer.update_timestamp as u64),
                )
                .unwrap();
            let step = buffer.step;
            let next_step = if duration < Self::SEGMENT_DURATION && step * 2 <= Self::MAX_STEP {
                step * 2
            } else if duration >= Self::SEGMENT_DURATION * 2 && step / 2 >= buffer.min_step {
                step / 2
            } else {
                step
            };
            tracing::info!(
                "Buffer[{}] step:{} duration:{:.2}mins next_step:{}",
                buffer.tag,
                step,
                duration.as_secs() as f64 / 60.0,
                next_step
            );
            let leaf = dao.update_max_by_step(buffer.tag, next_step).await?;
            buffer.update_timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            buffer.step = next_step;
            buffer.min_step = leaf.step;
            leaf
        };
        let step = buffer.step;
        let segment = if is_next {
            buffer.next_mut()
        } else {
            buffer.current_mut()
        };
        segment
            .val
            .store(leaf.max_id - step as i64, Ordering::Relaxed);
        segment.max = leaf.max_id;
        segment.step = step;
        Ok(())
    }

    async fn wait_and_sleep(&self, tag: i32) -> Result<()> {
        let mut roll = 0;
        let buffer = self.cache.get(&tag).ok_or(Error::TagNotExist)?.clone();
        let buffer = buffer.read().await;
        let delay_duration = Duration::from_millis(10);
        while buffer.thread_running.load(Ordering::Relaxed) {
            roll += 1;
            if roll > 10_000 {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "runtime-async-std")] {
                        async_std::task::sleep(delay_duration).await;
                    } else if #[cfg(feature = "runtime-tokio")] {
                        tokio::time::delay_for(delay_duration).await;
                    }
                }
                break;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Segment {
    pub val: AtomicI64,
    pub max: i64,
    pub step: i32,
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment(val={:?},max={},step={})",
            self.val, self.max, self.step
        )
    }
}

impl Segment {
    pub fn new(val: i64, max: i64, step: i32) -> Self {
        Self {
            val: val.into(),
            max,
            step,
        }
    }
    #[inline]
    pub fn idle(&self) -> i64 {
        self.max - min(self.max, self.val.load(Ordering::Relaxed)) // to prevent overflow
    }
}

#[derive(Debug)]
pub struct SegmentBuffer {
    pub init_ok: bool,
    pub next_ready: bool,
    pub(crate) thread_running: AtomicBool,
    pub(crate) update_timestamp: u128,
    pub(crate) step: i32,
    pub(crate) min_step: i32,
    pub tag: i32,
    segments: [Segment; 2],
    current_idx: usize,
}

impl SegmentBuffer {
    #[inline]
    pub fn new(tag: i32) -> Self {
        Self {
            init_ok: false,
            next_ready: false,
            thread_running: false.into(),
            update_timestamp: 0,
            step: 0,
            min_step: 0,
            tag,
            segments: [Segment::default(), Segment::default()],
            current_idx: 0,
        }
    }

    #[inline]
    pub fn current(&self) -> &Segment {
        &self.segments[self.current_idx]
    }

    #[inline]
    pub fn current_mut(&mut self) -> &mut Segment {
        &mut self.segments[self.current_idx]
    }

    #[inline]
    pub fn current_idx(&self) -> usize {
        self.current_idx
    }

    #[inline]
    pub fn next(&self) -> &Segment {
        &self.segments[self.next_idx()]
    }

    #[inline]
    pub fn next_mut(&mut self) -> &mut Segment {
        &mut self.segments[self.next_idx()]
    }

    #[inline]
    pub fn next_idx(&self) -> usize {
        (self.current_idx + 1) % 2
    }

    #[inline]
    pub fn switch(&mut self) {
        self.current_idx = self.next_idx();
    }
}
