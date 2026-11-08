//! A global allocator that counts live bytes.
//!
//! The count is off by default. When it is off, each allocation does one extra
//! relaxed load. The driver turns the count on only for a memory trial, and it
//! reports no timing from that trial.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};

/// True while the count is on.
static ENABLED: AtomicBool = AtomicBool::new(false);
/// The live bytes of the global allocator.
static COUNTER: Counter = Counter::new();

/// Counts live bytes, and keeps the highest count.
struct Counter {
    /// The live bytes counted so far.
    current: AtomicI64,
    /// The highest value of `current` since the last [`Counter::reset_peak`].
    peak: AtomicI64,
}

impl Counter {
    /// Makes a counter at zero.
    const fn new() -> Self {
        Self {
            current: AtomicI64::new(0),
            peak: AtomicI64::new(0),
        }
    }

    /// Counts `n` new live bytes and updates the peak.
    fn add(&self, n: usize) {
        let now = self.current.fetch_add(n as i64, Relaxed) + n as i64;
        self.peak.fetch_max(now, Relaxed);
    }

    /// Counts `n` freed bytes.
    fn sub(&self, n: usize) {
        self.current.fetch_sub(n as i64, Relaxed);
    }

    /// Counts a block that changed from `old` to `new` bytes.
    ///
    /// This counts only the difference. A `realloc` that moves the block has
    /// both blocks live for a short time, and the peak does not show that.
    fn resize(&self, old: usize, new: usize) {
        if new >= old {
            self.add(new - old);
        } else {
            self.sub(old - new);
        }
    }

    /// Returns the live bytes counted so far.
    fn current(&self) -> i64 {
        self.current.load(Relaxed)
    }

    /// Returns the highest count since the last [`Self::reset_peak`].
    fn peak(&self) -> i64 {
        self.peak.load(Relaxed)
    }

    /// Sets the peak to the current count.
    fn reset_peak(&self) {
        self.peak.store(self.current(), Relaxed);
    }
}

/// Wraps the system allocator and counts live bytes.
pub struct CountingAlloc;

// SAFETY: every call forwards to `System` with the same arguments. The counters
// do not allocate.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Relaxed) {
            COUNTER.add(layout.size());
        }
        // SAFETY: the caller's contract for `alloc` applies here.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Relaxed) {
            COUNTER.sub(layout.size());
        }
        // SAFETY: the caller's contract for `dealloc` applies here.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract for `realloc` applies here.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        // A failed `realloc` keeps the old block, so the count does not change.
        if !new_ptr.is_null() && ENABLED.load(Relaxed) {
            COUNTER.resize(layout.size(), new_size);
        }
        new_ptr
    }
}

/// Keeps the count on while it lives. The count stops on drop, also on an early
/// error return.
#[must_use = "the count stops when this value drops"]
pub struct Counting {
    /// The live bytes at [`Counting::start`].
    start: i64,
}

impl Counting {
    /// Turns the count on, and sets the peak to the current value.
    pub fn start() -> Self {
        COUNTER.reset_peak();
        ENABLED.store(true, Relaxed);
        Self {
            start: COUNTER.current(),
        }
    }

    /// Returns the live bytes counted so far.
    ///
    /// The value can be negative, because a block allocated while the count was
    /// off can be freed while it is on. So use only the difference of two calls.
    /// In a binary without [`CountingAlloc`], the value stays at zero.
    pub fn current(&self) -> i64 {
        COUNTER.current()
    }

    /// Returns the highest live bytes since [`Self::start`], above the value at
    /// the start. Earlier trials leave the count at a different value, so the
    /// absolute value would depend on the order of the trials.
    pub fn peak(&self) -> i64 {
        COUNTER.peak() - self.start
    }
}

impl Drop for Counting {
    fn drop(&mut self) {
        ENABLED.store(false, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::Counter;

    /// A block that shrinks in place counts only the difference.
    #[test]
    fn a_resize_counts_only_the_difference() {
        let counter = Counter::new();
        counter.add(64);
        counter.resize(64, 32);
        assert_eq!((counter.current(), counter.peak()), (32, 64));
        counter.resize(32, 48);
        assert_eq!((counter.current(), counter.peak()), (48, 64));
    }

    /// The peak starts again at the current count.
    #[test]
    fn reset_peak_starts_at_the_current_count() {
        let counter = Counter::new();
        counter.add(100);
        counter.sub(60);
        counter.reset_peak();
        assert_eq!(counter.peak(), 40);
        counter.add(10);
        assert_eq!(counter.peak(), 50);
    }
}
