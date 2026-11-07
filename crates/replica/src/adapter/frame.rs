//! The sync message of the Yrs and Loro adapters.
//!
//! Neither library has a sync protocol of its own, so these adapters add one. It
//! follows the Yjs sync protocol: each peer sends its version, and the other peer
//! answers with the data that this version does not have. A message can carry
//! both parts, so one message can answer a version and send data.
//!
//! The Automerge adapter uses Automerge's own sync protocol instead.

use anyhow::{Context, ensure};

/// The flag bit that says the message has a version.
const HAS_VERSION: u8 = 1;
/// The flag bit that says the message has an update.
const HAS_UPDATE: u8 = 2;
/// The flag bit that asks the receiver to send its version.
const ASKS_VERSION: u8 = 4;

/// One sync message.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Frame {
    /// The encoded version of the sender: a Yrs state vector or a Loro version
    /// vector.
    pub version: Option<Vec<u8>>,
    /// True if the sender does not know the version of the receiver, and so
    /// cannot send it data.
    pub asks_version: bool,
    /// An encoded update.
    pub update: Option<Vec<u8>>,
}

impl Frame {
    /// Encodes the frame as a flag byte, the version with a 4-byte length, and
    /// then the update.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut flags = 0;
        if self.version.is_some() {
            flags |= HAS_VERSION;
        }
        if self.update.is_some() {
            flags |= HAS_UPDATE;
        }
        if self.asks_version {
            flags |= ASKS_VERSION;
        }
        let mut out = vec![flags];
        if let Some(version) = &self.version {
            let len = u32::try_from(version.len()).expect("a version is below 4 GiB");
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(version);
        }
        if let Some(update) = &self.update {
            out.extend_from_slice(update);
        }
        out
    }

    /// Decodes a frame that [`Self::encode`] made.
    pub(crate) fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let (&flags, mut rest) = bytes.split_first().context("empty sync message")?;
        ensure!(
            flags & !(HAS_VERSION | HAS_UPDATE | ASKS_VERSION) == 0,
            "unknown sync message flags {flags:#x}"
        );
        let version = if flags & HAS_VERSION != 0 {
            ensure!(
                rest.len() >= 4,
                "sync message is too short for its version length"
            );
            let (len, tail) = rest.split_at(4);
            let len = u32::from_be_bytes(len.try_into().expect("four bytes")) as usize;
            ensure!(
                tail.len() >= len,
                "sync message is too short for its version"
            );
            let (version, tail) = tail.split_at(len);
            rest = tail;
            Some(version.to_vec())
        } else {
            None
        };
        let update = if flags & HAS_UPDATE != 0 {
            Some(rest.to_vec())
        } else {
            ensure!(rest.is_empty(), "sync message has bytes after its version");
            None
        };
        Ok(Self {
            version,
            asks_version: flags & ASKS_VERSION != 0,
            update,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Frame;

    #[test]
    fn every_kind_of_frame_survives_a_round_trip() {
        for version in [None, Some(vec![]), Some(vec![1, 2, 3])] {
            for update in [None, Some(vec![]), Some(vec![9; 300])] {
                for asks_version in [false, true] {
                    let frame = Frame {
                        version: version.clone(),
                        asks_version,
                        update: update.clone(),
                    };
                    assert_eq!(Frame::decode(&frame.encode()).unwrap(), frame);
                }
            }
        }
    }

    #[test]
    fn a_damaged_frame_is_an_error() {
        assert!(Frame::decode(&[]).is_err());
        assert!(Frame::decode(&[0x80]).is_err());
        assert!(Frame::decode(&[1, 0, 0, 0, 5, 1]).is_err());
        assert!(Frame::decode(&[0, 7]).is_err());
    }
}
