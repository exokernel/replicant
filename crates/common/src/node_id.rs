//! [`NodeId`], the identity of a replica.

use std::sync::Arc;

use anyhow::Context as _;

/// The stable identity of a replica, for example `node-0` or `node-1`.
///
/// The same type names a replica's own `actor_id` and the peers it connects
/// to. Both are in one namespace.
///
/// # Invariant
///
/// A `NodeId` is always a legal HTTP header value. The replica sends it as the
/// `x-peer-id` gRPC metadata header on each outbound sync stream.
/// [`NodeId::new`] is the only constructor, and it checks this.
///
/// # Lookup by `&str`
///
/// `NodeId` implements `Borrow<str>`. You can query a `HashMap<NodeId, _>`
/// with a plain `&str`. This lets [`CrdtAdapter`](crate::CrdtAdapter) keep its
/// `peer: &str` parameters. Adapters use the id only as an opaque map key.
///
/// # Cheap clones
///
/// The string is an `Arc<str>`, so a clone does not allocate. The replica
/// clones ids on each sync flush and metric record. Use
/// `Arc::<str>::from(&id)` to get the shared string, for example as a metric
/// attribute value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(Arc<str>);

impl NodeId {
    /// Validates and wraps a node identifier.
    ///
    /// Rejects the empty string and any value that is not a legal HTTP header
    /// value. A bad id thus fails where it enters the system, not at the first
    /// peer connection.
    pub fn new(id: impl Into<String>) -> anyhow::Result<Self> {
        let id = id.into();
        if id.is_empty() {
            anyhow::bail!("node id must not be empty");
        }
        id.parse::<tonic::metadata::AsciiMetadataValue>()
            .with_context(|| format!("node id is not a valid HTTP-header value: {id:?}"))?;
        Ok(Self(id.into()))
    }

    /// Returns the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the id as a gRPC metadata value, for the `x-peer-id` header.
    ///
    /// Cannot fail: [`NodeId::new`] already parsed the id as a header value.
    pub fn header_value(&self) -> tonic::metadata::AsciiMetadataValue {
        self.0
            .parse()
            .expect("NodeId validated as a header value at construction")
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for NodeId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Lets you query `HashMap<NodeId, V>` and `HashSet<NodeId>` with `&str`.
///
/// This is correct only because `NodeId`'s `Hash` and `Eq` delegate to the
/// inner `str`. Do not give `NodeId` a custom `Hash` or `Eq`.
impl std::borrow::Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> String {
        id.0.as_ref().to_owned()
    }
}

/// Returns the shared string without a copy.
impl From<&NodeId> for Arc<str> {
    fn from(id: &NodeId) -> Arc<str> {
        Arc::clone(&id.0)
    }
}

impl TryFrom<String> for NodeId {
    type Error = anyhow::Error;
    fn try_from(s: String) -> anyhow::Result<Self> {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_accepts_the_scheme_the_system_actually_uses() {
        for name in ["node-0", "node-11", "replica-0"] {
            let id = NodeId::new(name).unwrap();
            assert_eq!(id.as_str(), name);
            assert_eq!(id.to_string(), name);
            // The header conversion must not panic for any id that `new`
            // accepts.
            let _ = id.header_value();
        }
    }

    #[test]
    fn node_id_rejects_what_would_break_the_peer_header() {
        // The empty string, and control characters that are illegal in an
        // HTTP header value. A newline is the important case: it could inject
        // a second header. Horizontal tab and bytes 0x80-0xFF are legal in a
        // header value, so they are not in this list. See the next test.
        for bad in ["", "node\n0", "node\r\n0", "node\u{0}0"] {
            assert!(
                NodeId::new(bad).is_err(),
                "should have rejected {bad:?} as a node id"
            );
        }
    }

    /// `NodeId` accepts non-ASCII ids. Bytes 0x80-0xFF are legal obs-text in
    /// an HTTP header value, so the id can still go in `x-peer-id`.
    ///
    /// The name `AsciiMetadataValue` suggests a stricter rule. This test
    /// records the actual rule.
    #[test]
    fn node_id_allows_non_ascii_because_the_header_does() {
        let id = NodeId::new("nodé-0").unwrap();
        let _ = id.header_value();
    }

    #[test]
    fn node_id_maps_can_be_probed_with_a_str() {
        // The `Borrow<str>` impl makes this work. Without it, each lookup
        // would allocate a `NodeId`.
        let mut m = std::collections::HashMap::new();
        m.insert(NodeId::new("node-1").unwrap(), 7);
        assert_eq!(m.get("node-1"), Some(&7));
        assert_eq!(m.get("node-2"), None);
    }

    #[test]
    fn node_id_clones_share_one_string() {
        let id = NodeId::new("node-1").unwrap();
        let copy = id.clone();
        assert!(Arc::ptr_eq(
            &Arc::<str>::from(&id),
            &Arc::<str>::from(&copy)
        ));
        // The derived `Debug` prints the same text as for a `String` field.
        assert_eq!(format!("{id:?}"), r#"NodeId("node-1")"#);
    }
}
