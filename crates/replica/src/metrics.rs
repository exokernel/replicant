//! OTel metric instruments for the replica.
//!
//! All instruments use the `replicant.*` namespace. Attributes are passed at
//! record time (not baked into the instrument) so one instance covers every
//! actor/op/peer combination.
//!
//! The scaffolding layer owns these instruments; adapter implementations must
//! not record metrics directly.

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

/// Metric instruments recorded by the replica's gRPC handlers.
pub struct Metrics {
    /// Per-op application latency in milliseconds.
    /// Attributes: `actor`, `op`.
    pub op_duration_ms: Histogram<f64>,

    /// Outbound sync messages sent to a peer.
    /// Attributes: `actor`, `peer`.
    pub sync_tx: Counter<u64>,

    /// Inbound sync messages received from a peer.
    /// Attributes: `actor`, `peer`.
    pub sync_rx: Counter<u64>,

    /// Flushes skipped because the peer's outbound channel was full. The
    /// change stays pending for a later flush, so this counts delay, not loss —
    /// but a non-zero value means a peer is not draining fast enough and
    /// convergence timings for that run include the resulting stalls.
    /// Attributes: `actor`, `peer`.
    pub sync_deferred: Counter<u64>,

    /// Serialized document size in bytes, sampled after each op.
    /// Attributes: `actor`.
    pub doc_size_bytes: Gauge<u64>,
}

impl Metrics {
    /// Create all instruments from `meter`. Call once at startup.
    pub fn new(meter: &Meter) -> Self {
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
