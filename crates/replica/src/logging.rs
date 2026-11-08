//! The log filter of the replica and the orchestrator.
//!
//! **Keep the CRDT libraries at WARN.** Automerge and Loro put `#[instrument]`
//! spans on their hot paths, at the INFO level. When a span is enabled, the
//! subscriber formats each recorded argument with `Debug`. Automerge's
//! `load_changes` span records the whole change graph: on the B4 paper that is
//! 259,779 change hashes, formatted again on each received change. That made
//! one received change cost about 125 ms instead of about 21 ms, and it set the
//! Automerge heal floor of the docker and in-process lanes. The replay tool
//! installs no subscriber, so it did not pay this cost.

use tracing_subscriber::EnvFilter;

/// The library crates that must stay at WARN unless `RUST_LOG` names them.
const LIBRARY_CRATES: [&str; 4] = ["automerge", "loro", "loro_internal", "yrs"];

/// Returns the log filter: `RUST_LOG` if it is set, or `info` if it is not,
/// with each CRDT library crate (`automerge`, `loro`, `loro_internal`, `yrs`)
/// at WARN. A crate that `RUST_LOG` names keeps the level that `RUST_LOG` gives
/// it.
pub fn filter() -> EnvFilter {
    let env = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default();
    EnvFilter::new(spec(&env))
}

/// Returns the filter directives for the value `env` of `RUST_LOG`.
///
/// A crate that `env` names keeps the level that `env` gives it. So
/// `RUST_LOG=info,automerge=debug` shows Automerge's spans, and then pays their
/// cost.
fn spec(env: &str) -> String {
    let base = if env.is_empty() { "info" } else { env };
    let libraries = LIBRARY_CRATES
        .iter()
        .filter(|krate| !base.contains(*krate))
        .map(|krate| format!("{krate}=warn"));
    std::iter::once(base.to_owned())
        .chain(libraries)
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::spec;

    #[test]
    fn libraries_are_at_warn_by_default() {
        assert_eq!(
            spec(""),
            "info,automerge=warn,loro=warn,loro_internal=warn,yrs=warn"
        );
    }

    #[test]
    fn rust_log_is_kept_and_libraries_are_added() {
        assert_eq!(
            spec("debug"),
            "debug,automerge=warn,loro=warn,loro_internal=warn,yrs=warn"
        );
    }

    #[test]
    fn a_named_library_keeps_its_level() {
        assert_eq!(
            spec("info,automerge=debug"),
            "info,automerge=debug,loro=warn,loro_internal=warn,yrs=warn"
        );
    }
}
