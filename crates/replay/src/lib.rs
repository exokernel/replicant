//! Replays a recorded workload through a CRDT library and checks the result.
//!
//! A workload has two parts:
//!
//! - A **trace** ([`trace`]): which replica applies which ops, and what each
//!   replica has seen before it applies them. The format is a superset of the
//!   editing-traces format, so upstream files load unchanged.
//! - A **spec** ([`spec`]): how to run a trace. It says which part is timed,
//!   which replicas only receive, and which file hash to expect.
//!
//! [`driver`] runs a spec on a [`target`]. A target is an adapter in this
//! process, or a replica behind gRPC, such as one in a container. Every trial
//! must pass its [`oracle`] check. A trial that fails the check gives no timing.
//!
//! See `docs/replay.md`.

pub mod alloc;
pub mod driver;
pub mod oracle;
pub mod output;
pub mod spec;
pub mod target;
pub mod trace;
