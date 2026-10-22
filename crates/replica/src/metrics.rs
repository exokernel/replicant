//! OTel metric instruments for the replica.
//!
//! All instruments use the `replicant.*` namespace. Attributes are given at
//! record time, so one instrument covers every actor, op, and peer.
//!
//! The replica scaffolding owns these instruments. Adapters must not record
//! metrics.

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

/// Metric instruments recorded by the replica's gRPC handlers.
pub(crate) struct Metrics {
    /// Per-op application latency in milliseconds.
    /// Attributes: `actor`, `op`.
    pub(crate) op_duration_ms: Histogram<f64>,

    /// Outbound sync messages sent to a peer.
    /// Attributes: `actor`, `peer`.
    pub(crate) sync_tx: Counter<u64>,

    /// Inbound sync messages received from a peer.
    /// Attributes: `actor`, `peer`.
    pub(crate) sync_rx: Counter<u64>,

    /// Flushes skipped because the peer's outbound channel was full.
    ///
    /// The change stays pending for a later flush, so this counts delay, not
    /// loss. A non-zero value means a peer does not drain fast enough. The
    /// convergence times for that run then include those stalls.
    /// Attributes: `actor`, `peer`.
    pub(crate) sync_deferred: Counter<u64>,

    /// Serialized document size in bytes, sampled after each op.
    /// Attributes: `actor`.
    pub(crate) doc_size_bytes: Gauge<u64>,
}

impl Metrics {
    /// Create all instruments from `meter`. Call once at startup.
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            op_duration_ms: meter
                .f64_histogram("replicant.op.duration")
                .with_description("Per-op application latency")
                .with_unit("ms")
                .build(),
            sync_tx: meter
                .u64_counter("replicant.sync.messages.tx")
                .with_description("Outbound sync messages sent to a peer")
                .build(),
            sync_rx: meter
                .u64_counter("replicant.sync.messages.rx")
                .with_description("Inbound sync messages received from a peer")
                .build(),
            sync_deferred: meter
                .u64_counter("replicant.sync.messages.deferred")
                .with_description("Sync flushes skipped because the peer channel was full")
                .build(),
            doc_size_bytes: meter
                .u64_gauge("replicant.doc.size_bytes")
                .with_description("Serialized document size after each op")
                .with_unit("By")
                .build(),
        }
    }
}
