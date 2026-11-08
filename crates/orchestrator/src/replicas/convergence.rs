//! Reads replicas: polls fingerprints for convergence, and collects statistics.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tonic::Request;

use common::proto::{Empty, ObjRef};

use crate::event_log::{Event, EventLog, PeerStats};

use super::Client;
use super::nodes::node_index;
use super::writes::TEXT_OBJ;

/// Returns the replica's state fingerprint. An empty fingerprint means the
/// document has no operations yet.
pub async fn fingerprint(client: &mut Client) -> Result<Vec<u8>> {
    Ok(client
        .get_state_fingerprint(Request::new(Empty {}))
        .await?
        .into_inner()
        .fingerprint)
}

/// Polls the nodes at `indices` until every fingerprint is equal and non-empty.
///
/// Returns the milliseconds elapsed from `start`. Fails if `timeout` passes
/// first. Records each poll, and the convergence, in `log`.
pub async fn wait_for_nodes(
    clients: &mut [Client],
    indices: &[usize],
    start: Instant,
    timeout: Duration,
    log: &mut EventLog,
) -> Result<f64> {
    loop {
        let mut fps: Vec<Vec<u8>> = Vec::with_capacity(indices.len());
        for &i in indices {
            fps.push(fingerprint(&mut clients[i]).await?);
        }
        log.poll(indices, &fps);

        // An empty fingerprint means the node has no operations yet. Treat that
        // as not converged, so a run cannot match before any write lands.
        let converged = fps.iter().all(|fp| !fp.is_empty()) && fps.windows(2).all(|w| w[0] == w[1]);
        if converged {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            log.record(Event::Converged {
                nodes: indices.to_vec(),
            });
            return Ok(elapsed_ms);
        }

        if start.elapsed() >= timeout {
            bail!(
                "nodes {:?} did not converge within {}s",
                indices,
                timeout.as_secs()
            );
        }

        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Calls `GetStats` on each node and records the replies in `log`.
///
/// Call it after the timed window. Each call does a full encode of the document.
/// The call also makes the replica record its `replicant.doc.size_bytes` gauge,
/// so run it also when `log` is disabled.
pub async fn record_stats(clients: &mut [Client], log: &mut EventLog) -> Result<()> {
    for (node, client) in clients.iter_mut().enumerate() {
        let stats = client.get_stats(Request::new(Empty {})).await?.into_inner();
        if !log.enabled() {
            continue;
        }
        let peers = stats
            .peers
            .into_iter()
            .map(|p| {
                let peer = node_index(&p.peer_id)
                    .with_context(|| format!("unexpected peer ID {:?}", p.peer_id))?;
                Ok(PeerStats {
                    peer,
                    tx: p.tx,
                    rx: p.rx,
                    deferred: p.deferred,
                })
            })
            .collect::<Result<_>>()?;
        log.record(Event::Stats {
            node,
            doc_size_bytes: stats.doc_size_bytes,
            peers,
        });
    }
    Ok(())
}

/// Waits until sync is idle on every link in `edges`, or fails after `timeout`.
///
/// Sync is idle when no replica has a sync message to send, and each message that
/// a replica sent on a link has arrived at the other end. Each direction of each
/// link must also have carried at least one message, so the opening handshake is
/// done. The condition must hold on two polls in a row. A reply that a replica has
/// made but not yet counted cannot then pass as idle.
///
/// A timer alone cannot show this. An Automerge replica can compute for hundreds of
/// milliseconds between two messages of one handshake.
pub async fn wait_for_sync_idle(
    clients: &mut [Client],
    edges: &[(usize, usize)],
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    let mut idle_polls = 0;
    loop {
        if sync_is_idle(clients, edges).await? {
            idle_polls += 1;
            if idle_polls == 2 {
                return Ok(());
            }
        } else {
            idle_polls = 0;
        }
        if start.elapsed() >= timeout {
            bail!(
                "sync on links {edges:?} was not idle within {}s",
                timeout.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Returns true if sync on every link in `edges` is idle now. See
/// [`wait_for_sync_idle`].
async fn sync_is_idle(clients: &mut [Client], edges: &[(usize, usize)]) -> Result<bool> {
    // The messages that each node sent to, and received from, each peer.
    let mut sent: HashMap<(usize, usize), u64> = HashMap::new();
    let mut received: HashMap<(usize, usize), u64> = HashMap::new();
    for (node, client) in clients.iter_mut().enumerate() {
        let pending = client
            .sync_pending(Request::new(Empty {}))
            .await
            .with_context(|| format!("SyncPending on node {node}"))?
            .into_inner();
        if !pending.peer_ids.is_empty() {
            return Ok(false);
        }
        let stats = client.get_stats(Request::new(Empty {})).await?.into_inner();
        for p in stats.peers {
            let peer = node_index(&p.peer_id)
                .with_context(|| format!("unexpected peer ID {:?}", p.peer_id))?;
            sent.insert((node, peer), p.tx);
            received.insert((node, peer), p.rx);
        }
    }
    for &(a, b) in edges {
        for (from, to) in [(a, b), (b, a)] {
            let out = sent.get(&(from, to)).copied().unwrap_or(0);
            let arrived = received.get(&(to, from)).copied().unwrap_or(0);
            if out == 0 || out != arrived {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Checks that every node's text holds exactly `expected` characters. Valid
/// only for insert-only workloads, where each operation adds one character.
///
/// Matching fingerprints prove the replicas *agree*. This proves they agree on
/// a document that still contains everyone's work. A bug that made both sides
/// converge on one side's text only would pass the fingerprint check and fail
/// here.
pub async fn verify_text_length(
    clients: &mut [Client],
    indices: &[usize],
    expected: usize,
) -> Result<()> {
    for &i in indices {
        let got = clients[i]
            .get_text_length(Request::new(ObjRef {
                obj: TEXT_OBJ.to_owned(),
            }))
            .await
            .with_context(|| format!("GetTextLength on node {i}"))?
            .into_inner()
            .length as usize;
        if got != expected {
            bail!(
                "text-length check failed on node {i}: expected {expected} chars, found \
                 {got} — some replicas' inserts were lost in the merge"
            );
        }
    }
    Ok(())
}
