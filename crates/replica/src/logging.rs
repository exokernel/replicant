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
/// A crate that `env` names as a whole keeps the level that `env` gives it. So
/// `RUST_LOG=info,automerge=debug` shows Automerge's spans, and then pays their
/// cost. A directive for a module or a span only, such as
/// `automerge::sync=debug` or `yrs[sync]=trace`, does not remove the WARN
/// default of its crate. The filter applies the more exact directive where it
/// matches, and WARN to the rest of the crate.
fn spec(env: &str) -> String {
    let base = if env.is_empty() { "info" } else { env };
    let named: Vec<&str> = base.split(',').filter_map(whole_target).collect();
    let libraries = LIBRARY_CRATES
        .iter()
        .filter(|krate| !named.contains(krate))
        .map(|krate| format!("{krate}=warn"));
    std::iter::once(base.to_owned())
        .chain(libraries)
        .collect::<Vec<_>>()
        .join(",")
}

/// Returns the target of one `RUST_LOG` directive, the text before `=`, if the
/// directive applies to the whole target. A directive with a span filter, such
/// as `yrs[sync]=trace`, gives `None`. A directive with only a level, such as
/// `debug`, gives the level, which names no crate.
fn whole_target(directive: &str) -> Option<&str> {
    let target = directive.split('=').next().unwrap_or_default().trim();
    (!target.contains('[')).then_some(target)
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

    /// A module directive does not remove the WARN default of its crate.
    #[test]
    fn a_module_directive_keeps_its_crate_at_warn() {
        assert_eq!(
            spec("info,automerge::sync=debug"),
            "info,automerge::sync=debug,automerge=warn,loro=warn,loro_internal=warn,yrs=warn"
        );
    }

    /// `loro_internal` is not `loro`, and a span filter does not name its whole
    /// crate.
    #[test]
    fn crate_names_match_exactly() {
        assert_eq!(
            spec("loro_internal=debug,yrs[sync]=trace"),
            "loro_internal=debug,yrs[sync]=trace,automerge=warn,loro=warn,yrs=warn"
        );
    }
}
