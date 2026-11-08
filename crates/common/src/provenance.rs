//! Facts about a run that a result file records. Each returns `None` if the fact
//! is not available, because a run must not fail for lack of a label.

use std::process::Command;

/// Returns the trimmed output of a command, or `None` if the command fails.
fn capture(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Returns the commit of the working directory.
#[must_use]
pub fn git_commit() -> Option<String> {
    capture("git", &["rev-parse", "HEAD"])
}

/// Returns true if the working tree has uncommitted changes. The run is then not
/// reproducible from the commit alone.
#[must_use]
pub fn git_dirty() -> Option<bool> {
    // `--porcelain` prints one line per modified path, so empty output means a
    // clean tree.
    capture("git", &["status", "--porcelain"]).map(|s| !s.is_empty())
}

/// Returns the name of this host.
#[must_use]
pub fn hostname() -> Option<String> {
    // On Linux, the measurement host, /proc is authoritative. Not every shell
    // exports HOSTNAME, and macOS has no /proc.
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| capture("hostname", &[]))
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

/// Returns the number of CPUs that this process can use.
pub fn cpus() -> Option<usize> {
    std::thread::available_parallelism().map(usize::from).ok()
}
