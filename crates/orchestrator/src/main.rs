//! Orchestrator: runs scenarios and reports how long each one took to converge.
//!
//! Given no file arguments, it runs three built-in regression scenarios. Given
//! file arguments, it runs each TOML scenario `--trials` times and writes
//! structured records to stdout:
//!
//! ```text
//! cargo run --bin orchestrator -- --trials 5 --output csv scenarios/full-mesh-n5.toml
//! ```
//!
//! Logs go to stderr, so stdout stays clean enough to pipe.
//!
//! # Modules
//!
//! - [`scenario`]: the scenario file schema, its validation, and the built-ins.
//! - [`topology`]: graph shapes and graph properties.
//! - [`workload`]: write targets, the seeded PRNG, and text insert positions.
//! - [`replicas`]: the gRPC helpers that both runners share.
//! - [`runner`] and [`partition_heal`]: the two scenario runners.
//! - [`contention`] and [`provenance`]: run metadata that needs no replica.
//! - [`output`], [`metrics`], and [`trace`]: what the run writes.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;

mod contention;
mod metrics;
mod output;
mod partition_heal;
mod provenance;
mod replicas;
mod runner;
mod scenario;
mod topology;
mod trace;
mod workload;

use output::OutputFormat;
use replica::adapter::Crdt;
use replicas::{NodeSource, ReplicaEndpoint};
use runner::RunResult;
use scenario::{ScenarioBody, ScenarioFile, builtin_scenarios};
use trace::Trace;

// ── CLI ─────────────────────────────────────────────────────────────────────

/// Command-line arguments.
#[derive(Parser, Debug)]
#[command(about = "CRDT convergence benchmark orchestrator")]
struct Args {
    /// Number of times to run each scenario (must be ≥ 1).
    #[arg(long, default_value_t = 1)]
    trials: usize,

    /// Output format for benchmark results written to stdout.
    #[arg(long, value_enum, default_value_t = OutputFormat::Csv)]
    output: OutputFormat,

    /// Write OTel metrics as JSON to this file once all scenarios finish. Covers
    /// sync message counts, operation latencies, and document sizes.
    ///
    /// This takes precedence over OTLP export. Without it, the orchestrator
    /// exports over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, and drops
    /// its metrics when it is not. See [`metrics::init_metrics`].
    #[arg(long)]
    metrics_file: Option<PathBuf>,

    /// Write a run-provenance file as JSON to this path: commit, host, build
    /// profile, seeds, and cell parameters.
    ///
    /// Keep it beside the result CSV, which records none of those. `results/` is
    /// gitignored, so without this file a stored CSV cannot be traced back to
    /// the code that produced it.
    ///
    /// It is written before the first trial, so an interrupted sweep still
    /// leaves a record of what it was going to run.
    #[arg(long)]
    provenance: Option<PathBuf>,

    /// Parse and validate the scenarios, write the provenance file, and exit
    /// without running any trials.
    ///
    /// Everything in that file comes from the config and the seeds, so cell
    /// metadata, including achieved contention, is available without paying for
    /// a sweep.
    #[arg(long)]
    dry_run: bool,

    /// Write a per-trial event log as JSON Lines to this path: the links,
    /// each write, each convergence poll, and the heal steps.
    ///
    /// It shows how a trial converged, for teaching and debugging. Recording
    /// adds a little work inside the timed window, so do not use traced runs
    /// for measurements.
    #[arg(long)]
    trace: Option<PathBuf>,

    /// Connect to externally-managed replicas instead of spawning in-process.
    ///
    /// Takes comma-separated `client_addr[=peer_addr]` entries. The orchestrator
    /// dials `client_addr`. Each replica gives `peer_addr` to its peers, and it
    /// defaults to `client_addr`.
    ///
    /// The two differ when the replicas sit in another network namespace. Under
    /// docker-compose the orchestrator uses published ports while containers
    /// reach each other by service DNS:
    ///
    /// ```text
    /// --replicas localhost:50051=replica-0:50051,localhost:50052=replica-1:50051
    /// ```
    ///
    /// Each external replica must be launched with actor ID `node-N`, matching
    /// the orchestrator's wiring scheme. A scenario uses the first
    /// `node_count` entries and resets the rest, so one stack can serve
    /// scenarios of every size up to its own.
    ///
    /// Every trial begins with a `Reset` call to each replica, so many scenarios
    /// and trials can share one long-lived stack with no state carried over.
    #[arg(long, value_delimiter = ',', value_parser = parse_replica_endpoint)]
    replicas: Vec<ReplicaEndpoint>,

    /// Which CRDT library backs the in-process replicas.
    ///
    /// Valid only without `--replicas`. An external replica chose its library
    /// from its own `--crdt` at startup, and the orchestrator cannot change it,
    /// so passing both flags is an error rather than a silent no-op. In the
    /// container lanes the deploy generators make the choice; see the `crdt`
    /// argument on `just bench-docker` and `just bench-k8s`.
    #[arg(long, value_parser = crdt_parser())]
    crdt: Option<Crdt>,

    /// TOML scenario files to run. Omit these to run the built-in scenarios.
    scenarios: Vec<PathBuf>,
}

/// Builds the clap parser for `--crdt`.
///
/// `PossibleValuesParser` supplies the accepted list to `--help` and to the
/// error message. `.map` turns the validated string back into a [`Crdt`], so
/// the args struct stays typed. Both read [`Crdt::ALL`], so a new backend
/// cannot be accepted without also appearing in `--help`.
///
/// This lives in the binary because it is a CLI concern. Deriving `ValueEnum`
/// on `Crdt` would push it into the library crate.
fn crdt_parser() -> impl clap::builder::TypedValueParser<Value = Crdt> {
    use clap::builder::TypedValueParser as _;
    clap::builder::PossibleValuesParser::new(Crdt::ALL.map(|c| c.as_str())).map(|s| {
        s.parse::<Crdt>()
            .expect("PossibleValuesParser restricts the input to Crdt::ALL")
    })
}

/// Parses one `client_addr[=peer_addr]` entry of the `--replicas` flag.
fn parse_replica_endpoint(s: &str) -> Result<ReplicaEndpoint, String> {
    let (client, peer) = s.split_once('=').unwrap_or((s, s));
    let (client, peer) = (client.trim(), peer.trim());
    if client.is_empty() || peer.is_empty() {
        return Err(format!("empty address in --replicas entry '{s}'"));
    }
    Ok(ReplicaEndpoint {
        client_addr: client.to_owned(),
        peer_addr: peer.to_owned(),
    })
}

// ── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr, so the structured records on stdout stay clean.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    // Reject the combination rather than ignore one flag. A sweep that quietly
    // ran the wrong library would still produce plausible-looking numbers.
    if !args.replicas.is_empty() && args.crdt.is_some() {
        bail!(
            "--crdt applies only to in-process replicas; with --replicas the \
             library is fixed by how each replica was launched (see the `crdt` \
             argument on `just bench-docker` / `just bench-k8s`)"
        );
    }
    let in_process_crdt = args.crdt.unwrap_or(Crdt::Automerge);
    if args.trials == 0 {
        bail!("--trials must be at least 1");
    }
    let (provider, file_exporter) = metrics::init_metrics(args.metrics_file.as_deref())?;
    opentelemetry::global::set_meter_provider(provider.clone());

    let scenarios = load_scenarios(&args.scenarios)?;

    if let Some(path) = &args.provenance {
        provenance::write(
            path,
            &provenance::RunMeta {
                scenarios: &scenarios,
                paths: &args.scenarios,
                trials: args.trials,
                replicas: &args.replicas,
                // The orchestrator knows the library only for the in-process
                // lane. For an external stack the bench scripts record it.
                crdt: args.replicas.is_empty().then(|| in_process_crdt.as_str()),
            },
        )?;
        tracing::info!(path = %path.display(), "run provenance written");
    }

    if args.dry_run {
        tracing::info!(
            scenarios = scenarios.len(),
            "dry run — scenarios parsed and validated, no trials executed"
        );
        return Ok(());
    }

    let mut trace_file = match &args.trace {
        Some(path) => Some(std::io::BufWriter::new(
            std::fs::File::create(path)
                .with_context(|| format!("creating trace file {}", path.display()))?,
        )),
        None => None,
    };

    if matches!(args.output, OutputFormat::Csv) {
        println!("{}", output::header());
    }

    for scenario in &scenarios {
        let node_count = scenario.node_count();
        let op_count = scenario.op_count();
        let mut trial_ms: Vec<f64> = Vec::with_capacity(args.trials);
        let mut wiring_ms: Vec<f64> = Vec::with_capacity(args.trials);
        // Kept from the last trial. `topology_kind`, `edge_count`, and
        // `diameter` are the same in every trial, so the summary row reuses
        // them.
        let mut last_result: Option<RunResult> = None;

        for t in 1..=args.trials {
            let source = if args.replicas.is_empty() {
                NodeSource::InProcess(in_process_crdt)
            } else {
                NodeSource::External(args.replicas.clone())
            };
            let mut trace = Trace::new(trace_file.is_some());
            let result = run_scenario_once(scenario, source, t, &mut trace).await?;
            if let Some(out) = &mut trace_file {
                trace.write_to(out, &scenario.name, t)?;
            }
            debug_assert_eq!(
                result.total_ops, op_count,
                "scenario '{}' trial {t}: runner reported {} ops, expected {}",
                scenario.name, result.total_ops, op_count,
            );
            trial_ms.push(result.convergence_ms);
            wiring_ms.push(result.wiring_ms);
            output::emit(
                args.output,
                &output::trial_row(&scenario.name, t, node_count, op_count, &result),
            );
            last_result = Some(result);
        }

        let shape = last_result.expect("trials >= 1 enforced above");
        output::emit(
            args.output,
            &output::summary_row(
                &scenario.name,
                args.trials,
                node_count,
                op_count,
                &shape,
                &output::stats(&trial_ms),
                output::stats(&wiring_ms).mean,
            ),
        );
    }

    // A dropped BufWriter ignores write errors, so flush explicitly.
    if let Some(mut out) = trace_file {
        std::io::Write::flush(&mut out).context("writing the trace file")?;
    }

    // Flush pending metrics before exiting. Shutting the provider down lets the
    // OTLP exporter send its last batch, which matters because the reader's
    // interval is longer than most runs. For the file exporter it writes the
    // final JSON snapshot.
    provider
        .shutdown()
        .map_err(|e| anyhow::anyhow!("metrics provider shutdown failed: {e:?}"))?;
    if file_exporter.is_some() {
        tracing::info!(path = ?args.metrics_file, "OTel metrics written");
    }
    Ok(())
}

// ── Scenario helpers ─────────────────────────────────────────────────────────

/// Reads and validates the scenario files at `paths`, or returns the built-in
/// scenarios when `paths` is empty.
///
/// Every scenario is validated here, before the provenance file is written and
/// before any trial runs. A bad file late in a sweep then fails at once.
fn load_scenarios(paths: &[PathBuf]) -> Result<Vec<ScenarioFile>> {
    let scenarios: Vec<ScenarioFile> = if paths.is_empty() {
        builtin_scenarios()
    } else {
        paths
            .iter()
            .map(|path| {
                let raw = std::fs::read_to_string(path)
                    .with_context(|| format!("reading {}", path.display()))?;
                toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
            })
            .collect::<Result<_>>()?
    };
    for scenario in &scenarios {
        scenario.validate()?;
    }
    Ok(scenarios)
}

/// Runs one scenario once and returns its result.
///
/// `repetition` is the 1-based trial index. It feeds the divergence generator's
/// seed, so each repetition of a text cell draws its own operation stream. That
/// variation is what the run-to-run spread measures. `MapPut` scenarios ignore
/// it.
async fn run_scenario_once(
    scenario: &ScenarioFile,
    source: NodeSource,
    repetition: usize,
    trace: &mut Trace,
) -> Result<RunResult> {
    tracing::info!(scenario = %scenario.name, "starting");
    let result = match &scenario.body {
        ScenarioBody::Topology(config) => runner::run(config, source, trace).await?,
        ScenarioBody::PartitionHeal(config) => {
            partition_heal::run_partition_heal(config, source, repetition, trace).await?
        }
    };
    tracing::info!(
        scenario = %scenario.name,
        convergence_ms = result.convergence_ms,
        "PASSED",
    );
    Ok(result)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_replica_endpoint ────────────────────────────────────────────

    #[test]
    fn replica_endpoint_single_addr_uses_same_for_client_and_peer() {
        let ep = parse_replica_endpoint("localhost:50051").unwrap();
        assert_eq!(ep.client_addr, "localhost:50051");
        assert_eq!(ep.peer_addr, "localhost:50051");
    }

    #[test]
    fn replica_endpoint_distinct_client_and_peer() {
        let ep = parse_replica_endpoint("localhost:50051=replica-0:50051").unwrap();
        assert_eq!(ep.client_addr, "localhost:50051");
        assert_eq!(ep.peer_addr, "replica-0:50051");
    }

    #[test]
    fn replica_endpoint_trims_whitespace_around_separator() {
        let ep = parse_replica_endpoint(" localhost:50051 = replica-0:50051 ").unwrap();
        assert_eq!(ep.client_addr, "localhost:50051");
        assert_eq!(ep.peer_addr, "replica-0:50051");
    }

    #[test]
    fn replica_endpoint_rejects_empty_side() {
        assert!(parse_replica_endpoint("=replica-0:50051").is_err());
        assert!(parse_replica_endpoint("localhost:50051=").is_err());
        assert!(parse_replica_endpoint("").is_err());
    }

    /// A file that parses but fails validation is rejected at load time, with
    /// its scenario named.
    #[test]
    fn load_scenarios_rejects_an_invalid_file() {
        let dir = std::env::temp_dir().join(format!("replicant-load-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.toml");
        std::fs::write(
            &path,
            "name = \"bad\"\n\
             [partition_heal]\n\
             node_count = 4\n\
             ops_per_group = 1\n\
             write_pattern = \"round_robin\"\n\
             groups = [{ nodes = [0, 0] }, { nodes = [2, 3] }]\n",
        )
        .unwrap();
        let err = load_scenarios(&[path]).unwrap_err();
        std::fs::remove_dir_all(&dir).ok();
        assert!(format!("{err:#}").contains("scenario 'bad'"), "{err:#}");
    }
}
