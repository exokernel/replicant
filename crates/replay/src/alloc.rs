//! A global allocator that counts live bytes.
//!
//! The count is off by default. When it is off, each allocation does one extra
//! relaxed load. The driver turns the count on only for a memory trial, and it
//! reports no timing from that trial.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};

/// True while the count is on.
static ENABLED: AtomicBool = AtomicBool::new(false);
/// The live bytes counted so far.
static CURRENT: AtomicI64 = AtomicI64::new(0);
/// The highest value of [`CURRENT`] since the last [`reset_peak`].
static PEAK: AtomicI64 = AtomicI64::new(0);

/// Wraps the system allocator and counts live bytes.
pub struct CountingAlloc;

/// Counts `n` new live bytes and updates the peak.
fn add(n: usize) {
    let now = CURRENT.fetch_add(n as i64, Relaxed) + n as i64;
    PEAK.fetch_max(now, Relaxed);
}

/// Counts `n` freed bytes.
fn sub(n: usize) {
    CURRENT.fetch_sub(n as i64, Relaxed);
}

// SAFETY: every call forwards to `System` with the same arguments. The counters
// do not allocate.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Relaxed) {
            add(layout.size());
        }
        // SAFETY: the caller's contract for `alloc` applies here.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Relaxed) {
            sub(layout.size());
        }
        // SAFETY: the caller's contract for `dealloc` applies here.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ENABLED.load(Relaxed) {
            add(new_size);
            sub(layout.size());
        }
        // SAFETY: the caller's contract for `realloc` applies here.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Turns the count on or off.
pub fn set_counting(on: bool) {
    ENABLED.store(on, Relaxed);
}

/// Returns true if the binary installed [`CountingAlloc`] and the count is on.
/// In a test binary the allocator is not installed, so the values stay at zero.
pub fn is_counting() -> bool {
    ENABLED.load(Relaxed)
}

/// Returns the live bytes counted so far. This can be negative, because a block
/// allocated while the count was off can be freed while it is on. Use only the
/// difference of two calls.
pub fn current() -> i64 {
    CURRENT.load(Relaxed)
}

/// Returns the peak of [`current`] since the last [`reset_peak`].
pub fn peak() -> i64 {
    PEAK.load(Relaxed)
}

/// Sets the peak to the current value.
pub fn reset_peak() {
    PEAK.store(CURRENT.load(Relaxed), Relaxed);
}
