//! [`common::CrdtAdapter`] implementations, one per backing CRDT library.
//!
//! Each submodule owns its own adapter struct and the `#[test]` wrappers
//! that claim generic conformance-suite functions from [`conformance`]. A
//! new adapter is done when every generic function it wraps passes — see
//! that module's doc comment for what "generic" and "wraps" mean here.

pub mod automerge;
pub mod yrs;

pub use automerge::AutomergeAdapter;
pub use yrs::YrsAdapter;

#[cfg(test)]
pub(crate) mod conformance;
