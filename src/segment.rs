use std::cmp::min;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Segment {
    pub val: AtomicU64,
    pub max: u64,
    pub step: u32,
}


impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Segment(val={:?},max={},step={})", self.val, self.max, self.step)
    }
}

impl Segment {
    pub fn new(val: u64, max: u64, step: u32) -> Self {
        Self {
            val: val.into(),
            max,
            step,
        }
    }
    #[inline]
    pub fn idle(&self) -> u64 {
        self.max - min(self.max, self.val.load(Ordering::Relaxed)) // to prevent overflow
    }
}

#[derive(Debug)]
pub struct SegmentBuffer {
    pub init_ok: bool,
    pub next_ready: bool,
    pub(crate) thread_running: AtomicBool,
    pub(crate) update_timestamp: u128,
    pub(crate) step: u32,
    pub(crate) min_step: u32,
    pub tag: u32,
    segments: [Segment; 2],
    current_idx: usize,
}

impl SegmentBuffer {
    #[inline]
    pub fn new(tag: u32) -> Self {
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