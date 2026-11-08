//! Tests of the gRPC surface of one replica, over a real loopback connection.
//!
//! The orchestrator's tests cover the calls of a normal run. These tests cover
//! the status code of each kind of bad request, and the replay calls for lists
//! and maps, which the orchestrator never makes.

use common::proto::{
    self, Empty, InstanceId, LoadRequest, MapPut, ObjRef, OpRequest, PeerIds, PeerLinkUpdate,
    PeerRef, ScalarValue, TxnRequest, op_request, replica_client::ReplicaClient,
    scalar_value::Value,
};
use replica::adapter::Crdt;
use replica::server::{self, ReplicaState};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Channel;
use tonic::{Code, Request};

/// A client of one replica.
type Client = ReplicaClient<Channel>;

/// Starts a replica with actor id `name` on a loopback port. Returns its
/// address and a client. The server runs until the test ends.
async fn start(crdt: Crdt, name: &str) -> (String, Client) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let state = ReplicaState::new(common::NodeId::new(name).unwrap(), crdt.build());
    tokio::spawn(server::router(state).serve_with_incoming(TcpListenerStream::new(listener)));
    let client = common::connect_replica(&addr).await.unwrap();
    (addr, client)
}

/// Returns a request for one op.
fn op(op: op_request::Op) -> OpRequest {
    OpRequest { op: Some(op) }
}

/// Returns a `MapPut` of an integer on ROOT.
fn put(key: &str, value: i64) -> OpRequest {
    op(op_request::Op::MapPut(MapPut {
        obj: String::new(),
        key: key.to_owned(),
        value: Some(ScalarValue {
            value: Some(Value::IntVal(value)),
        }),
    }))
}

/// Returns a `ListInsert` of a string at `index`.
fn list_insert(index: u64, value: &str) -> OpRequest {
    op(op_request::Op::ListInsert(proto::ListInsert {
        obj: "list".to_owned(),
        index,
        value: Some(ScalarValue {
            value: Some(Value::StrVal(value.to_owned())),
        }),
    }))
}

/// Returns an `ObjRef` for `obj`.
fn obj(obj: &str) -> Request<ObjRef> {
    Request::new(ObjRef {
        obj: obj.to_owned(),
    })
}

#[tokio::test]
async fn a_request_with_no_op_is_an_invalid_argument() {
    let (_, mut client) = start(Crdt::Automerge, "node-0").await;
    let err = client.apply_op(OpRequest { op: None }).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("missing op"), "{}", err.message());

    let err = client
        .apply_txn(TxnRequest {
            ops: vec![put("k", 1), OpRequest { op: None }],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    // The bad op stopped the whole transaction, so the document is empty.
    let fp = client.get_state_fingerprint(Empty {}).await.unwrap();
    assert!(fp.into_inner().fingerprint.is_empty());
}

#[tokio::test]
async fn a_peer_id_that_is_not_a_header_value_is_an_invalid_argument() {
    let (addr, mut client) = start(Crdt::Yrs, "node-0").await;
    let bad = "node 1\n".to_owned();
    let err = client
        .connect_peer(PeerRef {
            peer_id: bad.clone(),
            addr,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let err = client
        .set_peer_links(PeerLinkUpdate {
            peer_ids: vec!["node-1".to_owned(), bad.clone()],
            blocked: true,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let err = client
        .kick_sync(PeerIds {
            peer_ids: vec![bad],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

/// Automerge creates a root object with an op. So a replica that already has
/// changes, but not the text object, must refuse to create it: the object
/// would not be the same on every replica.
#[tokio::test]
async fn ensure_text_after_other_changes_is_a_failed_precondition() {
    let (_, mut client) = start(Crdt::Automerge, "node-0").await;
    client.apply_op(put("k", 1)).await.unwrap();
    let err = client.ensure_text(obj("text")).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn the_length_of_a_missing_text_is_a_failed_precondition() {
    for crdt in [Crdt::Automerge, Crdt::Yrs] {
        let (_, mut client) = start(crdt, "node-0").await;
        let err = client.get_text_length(obj("nothing")).await.unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{crdt}");
        let err = client.read_text(obj("nothing")).await.unwrap_err();
        assert_eq!(err.code(), Code::Internal, "{crdt}");
    }
}

/// Applies `ops` on `a` in one transaction, then copies the document to `b`
/// with `Encode` and `Load`.
async fn copy_after(a: &mut Client, b: &mut Client, ops: Vec<OpRequest>) {
    let txn = a.apply_txn(TxnRequest { ops }).await.unwrap().into_inner();
    assert!(!txn.update.is_empty());
    let state = a.encode(Empty {}).await.unwrap().into_inner().state;
    b.load(LoadRequest { state }).await.unwrap();
}

/// The replay calls for a list and a map, on every library: a transaction
/// gives an update, `Encode` then `Load` copies the document to a second
/// replica, and the reads give the same content on both. A reset between the
/// two parts keeps the list out of the root map, which `ReadMap` refuses.
#[tokio::test]
async fn the_replay_calls_round_trip_a_list_and_a_map() {
    for crdt in Crdt::ALL {
        let (_, mut a) = start(crdt, "node-0").await;
        let (_, mut b) = start(crdt, "node-1").await;
        for (client, id) in [(&mut a, 1), (&mut b, 2)] {
            client
                .set_instance_id(InstanceId { high: 0, low: id })
                .await
                .unwrap();
        }
        a.ensure_list(obj("list")).await.unwrap();
        copy_after(
            &mut a,
            &mut b,
            vec![list_insert(0, "y"), list_insert(0, "x")],
        )
        .await;
        for client in [&mut a, &mut b] {
            let list = client.read_list(obj("list")).await.unwrap().into_inner();
            let values: Vec<_> = list.values.into_iter().map(|v| v.value).collect();
            assert_eq!(
                values,
                [
                    Some(Value::StrVal("x".to_owned())),
                    Some(Value::StrVal("y".to_owned()))
                ],
                "{crdt}"
            );
        }

        for client in [&mut a, &mut b] {
            client.reset(Empty {}).await.unwrap();
        }
        copy_after(&mut a, &mut b, vec![put("k", -7), put("j", 3)]).await;
        for client in [&mut a, &mut b] {
            let map = client.read_map(obj("")).await.unwrap().into_inner();
            assert_eq!(map.values.len(), 2, "{crdt}");
            assert_eq!(map.values["k"].value, Some(Value::IntVal(-7)), "{crdt}");
        }
    }
}

/// An update that does not decode is an adapter error, not a crash.
#[tokio::test]
async fn an_update_that_does_not_decode_is_an_internal_error() {
    for crdt in Crdt::ALL {
        let (_, mut client) = start(crdt, "node-0").await;
        let err = client
            .apply_updates(proto::UpdatesRequest {
                updates: vec![vec![0xff; 16]],
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Internal, "{crdt}");
        let err = client
            .load(LoadRequest {
                state: vec![0xff; 16],
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Internal, "{crdt}");
    }
}

/// `SyncPending` names a connected peer that has a message waiting, and not a
/// blocked one.
#[tokio::test]
async fn sync_pending_skips_a_blocked_peer() {
    let (_, mut a) = start(Crdt::Loro, "node-0").await;
    let (b_addr, _b) = start(Crdt::Loro, "node-1").await;
    a.set_peer_links(PeerLinkUpdate {
        peer_ids: vec!["node-1".to_owned()],
        blocked: true,
    })
    .await
    .unwrap();
    a.connect_peer(PeerRef {
        peer_id: "node-1".to_owned(),
        addr: b_addr,
    })
    .await
    .unwrap();
    a.apply_op(put("k", 1)).await.unwrap();
    let pending = a.sync_pending(Empty {}).await.unwrap().into_inner();
    assert!(pending.peer_ids.is_empty(), "{:?}", pending.peer_ids);

    // Unblock without a kick: the adapter still has the change to send.
    a.set_peer_links(PeerLinkUpdate {
        peer_ids: vec!["node-1".to_owned()],
        blocked: false,
    })
    .await
    .unwrap();
    let pending = a.sync_pending(Empty {}).await.unwrap().into_inner();
    assert_eq!(pending.peer_ids, ["node-1"]);
}
