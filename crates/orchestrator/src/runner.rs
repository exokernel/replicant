//! Runs one steady-state topology scenario: wire the graph, apply the writes,
//! and wait for every replica to agree.
//!
//! The divergence sweep runs in [`crate::partition_heal`]. Both runners build
//! on the replica helpers in [`crate::replicas`] and report a [`RunResult`].

use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::task::JoinSet;

use crate::replicas::{
    NodeSource, acquire_nodes, apply_write, check_tasks, connect_edges, ensure_text_all, reset_all,
    verify_text_length, wait_for_nodes,
};
use crate::scenario::{TopologyConfig, Workload};
use crate::workload::target_node;

/// What one completed trial measured.
#[derive(Debug, Clone, Copy)]
pub struct RunResult {
    /// Milliseconds until every node reports the same fingerprint. The clock
    /// starts at the first write for a topology run, and at the first unblock
    /// for a partition-heal run.
    pub convergence_ms: f64,
    /// The part of `convergence_ms` spent on link setup rather than merging.
    ///
    /// `0.0` for topology runs, which wire every edge before the clock starts.
    /// A partition-heal run also wires its whole graph up front, so this covers
    /// only the round of unblock calls that opens the heal. It is small and
    /// does not grow with the number of healed links.
    ///
    /// Report it alongside `convergence_ms` rather than subtracting it. It is
    /// part of the heal, and it also serves as a host-noise signal.
    pub wiring_ms: f64,
    /// Total operations applied across all nodes.
    pub total_ops: usize,
    /// Stable name of the topology that produced this run, such as
    /// `"full_mesh"`, `"ring"`, or `"partition_heal"`. Used as a pivot key in
    /// the CSV and JSON output.
    pub topology_kind: &'static str,
    /// Undirected edges in the final wired graph.
    pub edge_count: usize,
    /// Diameter of the final wired graph. `0` for `n <= 1`.
    pub diameter: usize,
}

/// Runs one steady-state topology scenario and returns its measurements.
///
/// Acquires the nodes, wires the graph, applies the writes, and waits for every
/// node to agree. All wiring happens before the clock starts, so `wiring_ms` is
/// always `0.0` here.
pub async fn run(config: &TopologyConfig, source: NodeSource) -> Result<RunResult> {
    config.validate()?;

    let n = config.node_count;
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = acquire_nodes(source, n, &mut tasks).await?;

    // Reset before wiring peers so each trial starts from a clean slate even
    // when the same external stack is reused across many scenarios/trials.
    reset_all(&clients).await?;

    // Create the shared text object on every node before any edge exists. See
    // `ensure_text_all`. Without this each node creates its own object on first
    // write, and those creations then collide as a map-key conflict.
    if config.workload == Workload::TextSplice {
        ensure_text_all(&mut clients, 0..n).await?;
    }

    let edges = config.connections.edges(n);
    let edge_count = edges.len();
    let topology_kind = config.connections.kind();
    let diameter = config.connections.diameter(n);
    connect_edges(&clients, &endpoints, &edges).await?;
    check_tasks(&mut tasks)?;

    // Start the clock before the first write, so the measurement includes
    // propagation. Over loopback, sync finishes before the first poll returns,
    // so a clock started after the writes would always read 0.
    let all_nodes: Vec<usize> = (0..n).collect();
    let measure_start = Instant::now();
    for i in 0..config.op_count {
        let target = target_node(&config.write_pattern, i, &all_nodes);
        apply_write(&mut clients[target], config.workload, i).await?;
        // Pace between operations only. A sleep after the last one would delay
        // the first convergence poll and inflate `convergence_ms`.
        if config.op_interval_ms > 0 && i + 1 < config.op_count {
            tokio::time::sleep(Duration::from_millis(config.op_interval_ms)).await;
        }
    }
    check_tasks(&mut tasks)?;

    // The timeout covers the total pacing delay plus 5s for convergence. An
    // unpaced run therefore gets the plain 5s deadline.
    let pacing_budget =
        Duration::from_millis(config.op_interval_ms.saturating_mul(config.op_count as u64));
    let convergence_ms = wait_for_nodes(
        &mut clients,
        &all_nodes,
        measure_start,
        Duration::from_secs(5) + pacing_budget,
    )
    .await?;
    check_tasks(&mut tasks)?;

    // Validity gate, outside the measured window: the converged text must hold
    // one character per operation, because operations are insert-only.
    if config.workload == Workload::TextSplice {
        verify_text_length(&mut clients, &all_nodes, config.op_count).await?;
    }

    Ok(RunResult {
        convergence_ms,
        // Every edge is wired before `measure_start`, so no setup is inside the
        // reported window.
        wiring_ms: 0.0,
        total_ops: config.op_count,
        topology_kind,
        edge_count,
        diameter,
    })
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use replica::adapter::Crdt;

    /// A topology run wires every edge before the clock starts, so its
    /// `wiring_ms` is 0.0 and no stream setup is inside `convergence_ms`.
    #[tokio::test]
    async fn topology_run_reports_no_wiring_inside_the_window() {
        let config: TopologyConfig = toml::from_str(
            "node_count = 3\n\
             connections = \"full_mesh\"\n\
             write_pattern = \"round_robin\"\n\
             op_count = 3",
        )
        .expect("valid topology config");
        let result = run(&config, NodeSource::InProcess(Crdt::Automerge))
            .await
            .unwrap();
        assert_eq!(result.wiring_ms, 0.0);
    }
}
