//! Tests of the `Sync` service of one replica. Each test opens the stream
//! itself, as a peer would, so it can send what a real peer never sends.
//!
//! The block test is the partition guarantee at the lowest level: a replica
//! that has blocked a peer drops that peer's messages, even on an open stream.

use std::time::Duration;

use common::proto::{
    Empty, PeerLinkUpdate, PeerRef, SyncMessage, replica_client::ReplicaClient,
    sync_client::SyncClient,
};
use replica::adapter::Crdt;
use replica::server::{self, ReplicaState};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Channel;
use tonic::{Code, Request, Streaming};

/// The peer id that the tests use for themselves.
const PEER: &str = "node-9";

/// Starts a replica `node-0` with `crdt` on a loopback port. Returns its
/// address and a control client.
async fn start(crdt: Crdt) -> (String, ReplicaClient<Channel>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let state = ReplicaState::new(common::NodeId::new("node-0").unwrap(), crdt.build());
    tokio::spawn(server::router(state).serve_with_incoming(TcpListenerStream::new(listener)));
    let client = common::connect_replica(&addr).await.unwrap();
    (addr, client)
}

/// Opens a sync stream to the replica at `addr`, with `peer_id` as the
/// `x-peer-id` header if it is given. Returns the sender of the outbound half
/// and the result of the call.
async fn open(
    addr: &str,
    peer_id: Option<&str>,
) -> (
    mpsc::Sender<SyncMessage>,
    Result<Streaming<SyncMessage>, tonic::Status>,
) {
    let mut client = SyncClient::connect(format!("http://{addr}")).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    let mut request = Request::new(ReceiverStream::new(rx));
    if let Some(id) = peer_id {
        request
            .metadata_mut()
            .insert("x-peer-id", id.parse().unwrap());
    }
    let result = client
        .stream(request)
        .await
        .map(tonic::Response::into_inner);
    (tx, result)
}

/// Returns the messages that the replica has received from [`PEER`], from
/// `GetStats`. Zero if the replica does not list the peer.
async fn received(client: &mut ReplicaClient<Channel>) -> u64 {
    let stats = client.get_stats(Empty {}).await.unwrap().into_inner();
    stats
        .peers
        .iter()
        .find(|p| p.peer_id == PEER)
        .map_or(0, |p| p.rx)
}

/// Returns sync messages from a Yrs document with one write, which carry that
/// write to an empty Yrs replica.
///
/// A Yrs adapter sends data only to a peer whose version it knows. So the
/// sender first receives the version of an empty document, and its next
/// messages then carry the write.
fn yrs_messages_with_a_write() -> Vec<Vec<u8>> {
    let mut sender = Crdt::Yrs.build();
    let mut empty = Crdt::Yrs.build();
    sender
        .apply_op(&common::Op::MapPut {
            obj: String::new(),
            key: "k".into(),
            value: common::ScalarVal::Int(1),
        })
        .unwrap();
    let version = empty.sync_generate("node-0").unwrap();
    sender.sync_receive("node-0", version).unwrap();
    std::iter::from_fn(|| sender.sync_generate("node-0")).collect()
}

#[tokio::test]
async fn a_stream_without_a_peer_id_is_refused() {
    let (addr, _client) = start(Crdt::Yrs).await;
    let (_tx, result) = open(&addr, None).await;
    assert_eq!(result.unwrap_err().code(), Code::InvalidArgument);
    let (_tx, result) = open(&addr, Some("")).await;
    assert_eq!(result.unwrap_err().code(), Code::InvalidArgument);
}

#[tokio::test]
async fn connect_peer_to_a_closed_port_is_unavailable() {
    let (_, mut client) = start(Crdt::Automerge).await;
    // Bind and drop a listener, so the port is closed.
    let closed = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let err = client
        .connect_peer(PeerRef {
            peer_id: "node-1".to_owned(),
            addr: closed.to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
}

/// A message that the adapter cannot decode ends the stream. The replica keeps
/// serving.
#[tokio::test]
async fn a_message_that_does_not_decode_ends_the_stream() {
    let (addr, mut client) = start(Crdt::Loro).await;
    let (tx, result) = open(&addr, Some(PEER)).await;
    let mut inbound = result.unwrap();
    tx.send(SyncMessage {
        payload: vec![0xff; 8],
    })
    .await
    .unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), async {
        while inbound.next().await.is_some_and(|m| m.is_ok()) {}
    })
    .await;
    assert!(end.is_ok(), "the stream stayed open after a bad message");
    assert_eq!(received(&mut client).await, 0);
    client.get_state_fingerprint(Empty {}).await.unwrap();
}

/// While the replica blocks [`PEER`], a message from it is dropped: the
/// document does not change and the message is not counted. After the
/// unblock, the same message applies.
#[tokio::test]
async fn a_blocked_peer_cannot_change_the_document() {
    let (addr, mut client) = start(Crdt::Yrs).await;
    let block = |blocked| PeerLinkUpdate {
        peer_ids: vec![PEER.to_owned()],
        blocked,
    };
    client.set_peer_links(block(true)).await.unwrap();
    let (tx, result) = open(&addr, Some(PEER)).await;
    let _inbound = result.unwrap();
    let messages = yrs_messages_with_a_write();
    let fingerprint = |c: &mut ReplicaClient<Channel>| {
        let mut c = c.clone();
        async move {
            c.get_state_fingerprint(Empty {})
                .await
                .unwrap()
                .into_inner()
                .fingerprint
        }
    };

    for payload in messages.clone() {
        tx.send(SyncMessage { payload }).await.unwrap();
    }
    // Give the replica time to read the messages, then check nothing changed.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fingerprint(&mut client).await.is_empty());
    assert_eq!(received(&mut client).await, 0);

    client.set_peer_links(block(false)).await.unwrap();
    for payload in messages {
        tx.send(SyncMessage { payload }).await.unwrap();
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while fingerprint(&mut client).await.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the write never arrived"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(received(&mut client).await > 0);
}
