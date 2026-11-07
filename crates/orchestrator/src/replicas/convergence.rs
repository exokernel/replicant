//! Reads replicas: polls fingerprints for convergence, and collects statistics.

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
