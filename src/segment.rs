use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_mutex::Mutex;
use dashmap::DashMap;

use crate::{Error, LeafDao, Result};

use super::utils;

type Cache = Arc<DashMap<i32, Arc<Mutex<SegmentBuffer>>>>;

pub struct LeafSegment<D> {
    dao: Arc<D>,
    init_ok: bool,
    pub cache: Cache,
    config: Config,
}

impl<D: 'static + LeafDao + Send + Sync> LeafSegment<D> {
    pub fn new(dao: Arc<D>, config: Config) -> Self {
        Self {
            dao,
            init_ok: false,
            cache: Arc::new(DashMap::new()),
            config,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        tracing::info!("Init ...");
        if !self.config.is_lazy {
            Self::update_cache_with_db(self.cache.clone(), self.dao.clone()).await?;
        }
        self.init_ok = true;
        self.update_cache_periodically();
        Ok(())
    }

    pub async fn get(&self, tag: i32) -> Result<i64> {
        if !self.init_ok {
            return Err(Error::ServiceNotReady);
        }
        let buffer = self.get_segment_buffer(tag).await?;
        if !buffer.lock().await.init_ok {
            tracing::info!("Init Buffer[{}]", tag);
            Self::update_segment_from_db(self.dao.clone(), buffer, false, true, self.config)
                .await?;
        }
        self.get_id_from_segment_buffer(tag).await
    }

    async fn get_segment_buffer(&self, tag: i32) -> Result<Arc<Mutex<SegmentBuffer>>> {
        if let Some(entry) = self.cache.get(&tag) {
            Ok(entry.value().clone())
        } else if self.config.is_lazy {
            self.dao.leaf(tag).await?;
            if let Some(entry) = self.cache.get(&tag) {
                Ok(entry.value().clone())
            } else {
                let buffer = Arc::new(Mutex::new(SegmentBuffer::new(tag)));
                self.cache.insert(tag, buffer.clone());
                Ok(buffer)
            }
        } else {
            Err(Error::TagNotExist)
        }
    }

    fn update_cache_periodically(&self) {
        let cache = self.cache.clone();
        let dao = self.dao.clone();
        let interval = self.config.update_cache_interval;
        let is_lazy = self.config.is_lazy;
        utils::spawn(async move {
            loop {
                utils::sleep(interval).await;
                let result = if is_lazy {
                    Self::clean_cache(cache.clone()).await
                } else {
                    Self::update_cache_with_db(cache.clone(), dao.clone()).await
                };
                if let Err(err) = result {
                    tracing::error!("Update cache failed: {}", err);
                }
            }
        });
    }

    async fn clean_cache(cache: Cache) -> Result<()> {
        todo!()
    }

    async fn update_cache_with_db(cache: Cache, dao: Arc<D>) -> Result<()> {
        tracing::info!("Update cache with database");
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
            cache.insert(t, Arc::new(Mutex::new(SegmentBuffer::new(t))));
        }
        for t in remove_tags {
            tracing::info!("Remove tag[{}] from cache", t);
            cache.remove(&t);
        }
        Ok(())
    }

    async fn get_id_from_segment_buffer(&self, tag: i32) -> Result<i64> {
        let buffer_wrapped = self.get_segment_buffer(tag).await?;
        let buffer = buffer_wrapped.lock().await;
        let segment = buffer.current();
        if !buffer.next_ready
            && (segment.idle() < (segment.step * 9 / 10) as i64)
            && !buffer
                .thread_running
                .compare_and_swap(false, true, Ordering::Relaxed)
        {
            let dao = self.dao.clone();
            let buffer_wrapped = buffer_wrapped.clone();
            let config = self.config;
            utils::spawn(async move {
                if Self::update_segment_from_db(dao, buffer_wrapped.clone(), true, false, config)
                    .await
                    .is_ok()
                {
                    tracing::info!("Update Buffer[{}]'s next segment from DB", tag);
                    let mut buffer = buffer_wrapped.lock().await;
                    buffer.next_ready = true;
                }
                let buffer = buffer_wrapped.lock().await;
                buffer.thread_running.store(false, Ordering::Relaxed);
            });
        }
        let val = segment.val.fetch_add(1, Ordering::Relaxed);
        if val < segment.max {
            Ok(val)
        } else {
            drop(buffer); // prevent deadlock
            self.wait_and_sleep(tag).await?;
            let mut buffer = buffer_wrapped.lock().await;
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

    async fn update_segment_from_db(
        dao: Arc<D>,
        buffer: Arc<Mutex<SegmentBuffer>>,
        is_next: bool,
        is_init: bool,
        config: Config,
    ) -> Result<()> {
        let mut buffer = buffer.lock().await;
        if is_init && buffer.init_ok {
            return Ok(());
        }
        let leaf = if !buffer.init_ok {
            let leaf = dao.update_max(buffer.tag).await?;
            buffer.step = leaf.step;
            buffer.min_step = leaf.step;
            buffer.init_ok = true;
            buffer.updated_at = Instant::now();
            leaf
        } else {
            let duration = buffer.updated_at.elapsed();
            let step = buffer.step;
            let next_step = if duration < config.segment_duration && step * 2 <= config.max_step {
                step * 2
            } else if duration >= config.segment_duration * 2 && step / 2 >= buffer.min_step {
                step / 2
            } else {
                step
            };
            tracing::info!(
                "Buffer[{}] step:{} duration:{:.2}ms next_step:{}",
                buffer.tag,
                step,
                duration.as_millis(),
                next_step
            );
            let leaf = dao.update_max_by_step(buffer.tag, next_step).await?;
            buffer.updated_at = Instant::now();
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
        let buffer = self.get_segment_buffer(tag).await?;
        let buffer = buffer.lock().await;
        while buffer.thread_running.load(Ordering::Relaxed) {
            roll += 1;
            if roll > 10_000 {
                utils::sleep(Duration::from_millis(10)).await;
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
    #[inline]
    pub fn new(val: i64, max: i64, step: i32) -> Self {
        Self {
            val: val.into(),
            max,
            step,
        }
    }
    #[inline]
    pub fn idle(&self) -> i64 {
        self.max.saturating_sub(self.val.load(Ordering::Relaxed))
    }
}

#[derive(Debug)]
pub struct SegmentBuffer {
    pub init_ok: bool,
    pub next_ready: bool,
    pub(crate) thread_running: AtomicBool,
    pub(crate) updated_at: Instant,
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
            updated_at: Instant::now(),
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

/// Config of [`LeafSegment`]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Config {
    /// * default(`false`): load all tags from database at startup,
    /// and update with database every `update_cache_interval`.
    ///
    /// * lazy(`true`): load tags on demand, and clean cache every `update_cache_interval`.
    pub is_lazy: bool,
    /// upper bound of step, default is 1_000_000.
    pub max_step: i32,
    /// related to generate next step, default is 15min.
    pub segment_duration: Duration,
    /// behavior depends on `is_lazy`, default is 1min.
    pub update_cache_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            is_lazy: false,
            max_step: 1_000_000,
            segment_duration: Duration::from_secs(15 * 60),
            update_cache_interval: Duration::from_secs(60),
        }
    }
}

impl Config {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    #[inline]
    pub fn lazy() -> Self {
        Self {
            is_lazy: true,
            ..Self::default()
        }
    }
    #[inline]
    pub fn set_max_step(mut self, step: i32) -> Self {
        self.max_step = step;
        self
    }
    #[inline]
    pub fn set_segment_duration(mut self, duration: Duration) -> Self {
        self.segment_duration = duration;
        self
    }
    #[inline]
    pub fn set_update_cache_interval(mut self, interval: Duration) -> Self {
        self.update_cache_interval = interval;
        self
    }
}
