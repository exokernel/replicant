//! Sends writes to replicas.

use anyhow::{Context, Result};
use tonic::Request;

use common::proto::{MapPut, ObjRef, OpRequest, ScalarValue, TextSplice, op_request, scalar_value};

use crate::scenario::WriteKind;

use super::Client;

/// Name of the shared text object that every `TextSplice` workload writes to.
pub use common::TEXT_OBJ;

/// Applies one `MapPut` to the root map.
pub async fn map_put(client: &mut Client, key: &str, val: &str) -> Result<()> {
    client
        .apply_op(Request::new(OpRequest {
            op: Some(op_request::Op::MapPut(MapPut {
                obj: String::new(),
                key: key.to_owned(),
                value: Some(ScalarValue {
                    value: Some(scalar_value::Value::StrVal(val.to_owned())),
                }),
            })),
        }))
        .await?;
    Ok(())
}

/// Applies one `TextSplice` to the named text object under the root.
///
/// Inserts only; `del_count` is always 0. The caller supplies `pos` and must
/// keep it within the replica's current text length. `pos = 0` is valid at any
/// length.
pub async fn text_splice(client: &mut Client, obj: &str, pos: usize, insert: &str) -> Result<()> {
    client
        .apply_op(Request::new(OpRequest {
            op: Some(op_request::Op::TextSplice(TextSplice {
                obj: obj.to_owned(),
                pos: pos as u64,
                del_count: 0,
                insert: insert.to_owned(),
            })),
        }))
        .await?;
    Ok(())
}

/// Applies write number `seq` to `client`, using the configured `workload`.
///
/// `seq` keeps `MapPut` keys distinct. The text workload ignores it and
/// prepends one filler character, because merge cost depends on position and
/// operation shape, not on content.
///
/// The divergence sweep uses this for `MapPut` only. It draws its text
/// positions from the locality rule instead. See [`crate::partition_heal`].
pub async fn apply_write(client: &mut Client, workload: WriteKind, seq: usize) -> Result<()> {
    match workload {
        WriteKind::MapPut => map_put(client, &format!("k{seq}"), &format!("v{seq}")).await,
        WriteKind::TextSplice => text_splice(client, TEXT_OBJ, 0, "x").await,
    }
}

/// Creates the shared text object on every node in `indices`.
///
/// Each replica writes a bit-identical first change, using a fixed actor and
/// time 0. All replicas therefore end up with one shared object identity
/// without syncing.
///
/// Call this after `reset_all` and **before any edge is wired**. Once sync is
/// running, a peer's operation can arrive first, and the replica's determinism
/// guard then rejects the bootstrap change.
pub async fn ensure_text_all(
    clients: &mut [Client],
    indices: impl Iterator<Item = usize>,
) -> Result<()> {
    for i in indices {
        clients[i]
            .ensure_text(Request::new(ObjRef {
                obj: TEXT_OBJ.to_owned(),
            }))
            .await
            .with_context(|| format!("EnsureText on node {i}"))?;
    }
    Ok(())
}
