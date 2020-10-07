use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_mutex::{Mutex, MutexGuardArc};
use dashmap::DashMap;

use crate::{Error, LeafDao, Result};

use super::utils;
use event_listener::Event;

type Cache = Arc<DashMap<i32, Arc<Mutex<SegmentBuffer>>>>;

pub struct SegmentIDGen<D> {
    dao: Arc<D>,
    init_ok: bool,
    cache: Cache,
    config: Config,
}

impl<D: 'static + LeafDao + Send + Sync> SegmentIDGen<D> {
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
            Self::update_cache_from_db(self.cache.clone(), self.dao.clone()).await?;
        }
        self.init_ok = true;
        self.update_cache_periodically();
        Ok(())
    }

    /// Get an ID
    pub async fn get(&self, tag: i32) -> Result<i64> {
        if !self.init_ok {
            return Err(Error::ServiceNotReady);
        }
        let mut buffer = self.get_segment_buffer(tag).await?.lock_arc().await;
        if !buffer.init_ok {
            tracing::info!("Init Buffer[{}]", tag);
            buffer = Self::update_segment_from_db(
                self.dao.clone(),
                buffer,
                false,
                true,
                self.config,
            )
                .await?;
        }
        Self::get_id_from_segment_buffer(self.dao.clone(), self.config, buffer).await
    }

    /// Update from database
    pub async fn update(&self, tag: i32) -> Result<()> {
        let buffer = self.get_segment_buffer(tag).await?.lock_arc().await;
        Self::update_segment_from_db(self.dao.clone(), buffer, false, false, self.config).await?;
        Ok(())
    }

    /// Remove from cache, useful in lazy mode.
    pub async fn remove(&self, tag: i32) -> bool {
        self.cache.remove(&tag)
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
        if self.config.is_lazy {
            return;
        }
        let cache = self.cache.clone();
        let dao = self.dao.clone();
        let interval = self.config.update_cache_interval;
        utils::spawn(async move {
            loop {
                utils::sleep(interval).await;
                if let Err(err) = Self::update_cache_from_db(cache.clone(), dao.clone()).await {
                    tracing::error!("Update cache failed: {}", err);
                }
            }
        });
    }

    async fn update_cache_from_db(cache: Cache, dao: Arc<D>) -> Result<()> {
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

    async fn get_id_from_segment_buffer(
        dao: Arc<D>,
        config: Config,
        mut buffer: MutexGuardArc<SegmentBuffer>,
    ) -> Result<i64> {
        let tag = buffer.tag;
        let segment = buffer.current();
        if !buffer.next_ready
            && (segment.idle() < (segment.step * 9 / 10) as i64)
            && !buffer
            .bg_task_running
            .compare_and_swap(false, true, Ordering::Acquire)
        {
            buffer = Self::update_segment_from_db(dao, buffer, true, false, config).await?;
            tracing::info!("Update Buffer[{}]'s next segment from DB", tag);
            buffer.next_ready = true;
            buffer.bg_task_running.store(false, Ordering::Release);
            buffer.bg_task_finished.notify(std::usize::MAX);
        }
        let segment = buffer.current_mut();
        let val = segment.val;
        segment.val += 1;
        if val < segment.max {
            Ok(val)
        } else {
            // waits until background task finished
            buffer = if buffer.bg_task_running.load(Ordering::Acquire) {
                let listener = buffer.bg_task_finished.listen();
                let buffer_mutex = MutexGuardArc::source(&buffer).clone();
                drop(buffer);
                listener.await;
                buffer_mutex.lock_arc().await
            } else {
                buffer
            };
            let segment = buffer.current_mut();
            let val = segment.val;
            segment.val += 1;
            if val < segment.max {
                Ok(val)
            } else if buffer.next_ready {
                tracing::info!("Buffer[{}] switched", tag);
                buffer.switch();
                buffer.next_ready = false;
                let val = buffer.current_mut().val;
                buffer.current_mut().val += 1;
                Ok(val)
            } else {
                Err(Error::BothSegmentsNotReady)
            }
        }
    }

    async fn update_segment_from_db(
        dao: Arc<D>,
        mut buffer: MutexGuardArc<SegmentBuffer>,
        is_next: bool,
        is_init: bool,
        config: Config,
    ) -> Result<MutexGuardArc<SegmentBuffer>> {
        if is_init && buffer.init_ok {
            return Ok(buffer);
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
        segment.val = leaf.max_id - step as i64;
        segment.max = leaf.max_id;
        segment.step = step;
        Ok(buffer)
    }
}

#[derive(Debug, Default)]
pub struct Segment {
    pub val: i64,
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
        Self { val, max, step }
    }
    #[inline]
    pub fn idle(&self) -> i64 {
        self.max.saturating_sub(self.val)
    }
}

#[derive(Debug)]
pub struct SegmentBuffer {
    pub init_ok: bool,
    pub next_ready: bool,
    pub tag: i32,
    bg_task_running: AtomicBool,
    bg_task_finished: Event,
    updated_at: Instant,
    step: i32,
    min_step: i32,
    segments: [Segment; 2],
    current_idx: usize,
}

impl SegmentBuffer {
    #[inline]
    pub fn new(tag: i32) -> Self {
        Self {
            init_ok: false,
            next_ready: false,
            bg_task_running: false.into(),
            bg_task_finished: Event::new(),
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

/// Config of [`SegmentIDGen`]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Config {
    /// * default(`false`): load all tags from database at startup,
    /// and update with database every `update_cache_interval`.
    ///
    /// * lazy(`true`): load tags on demand, and needs remove them manually.
    pub is_lazy: bool,
    /// upper bound of step, default is 1_000_000.
    pub max_step: i32,
    /// related to generate next step, default is 15min.
    pub segment_duration: Duration,
    /// default is 1min.
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
