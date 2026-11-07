//! The `replay` binary. See the [`replay`] crate and `docs/replay.md`.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;
use replica::adapter::Crdt;
use serde::Serialize;
use serde_json::json;

use replay::alloc::CountingAlloc;
use replay::driver::{DEFAULT_ID_SEED, Ids, Plan, run_memory_trial, run_trial};
use replay::output::{self, Row, RowKey};
use replay::spec::{self, IdOrder, Workload};
use replay::target::Lane;
use replay::trace::{Trace, read_verified};

/// The allocator that a memory trial reads. See [`replay::alloc`].
#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

/// Replays workloads through CRDT libraries, and checks the final content.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// The spec file.
    #[arg(long, default_value = "workloads/crdt-benchmarks.toml")]
    spec: PathBuf,
    /// The directory that holds the trace files.
    #[arg(long, default_value = "traces")]
    data_dir: PathBuf,
    /// The order of the library ids across agents, for every workload. Omit this
    /// to use the order that each workload names, or else `seeded`.
    #[arg(long, value_enum)]
    id_order: Option<IdOrder>,
    /// The seed of `--id-order seeded`.
    #[arg(long, default_value_t = DEFAULT_ID_SEED)]
    id_seed: u64,
    /// Runs one trace file with default settings, and ignores the spec.
    #[arg(long, conflicts_with_all = ["spec", "only", "include_heavy"])]
    trace: Option<PathBuf>,
    /// Runs only these workloads. A name that ends in `.` selects a group, such
    /// as `b1.` for all of B1.
    #[arg(long, value_delimiter = ',')]
    only: Vec<String>,
    /// Also runs workloads that the spec marks as heavy.
    #[arg(long)]
    include_heavy: bool,
    /// The libraries to run. With `--replicas`, give the one library that the
    /// replicas use.
    #[arg(long, value_delimiter = ',', default_value = "automerge,yrs,loro")]
    crdt: Vec<Crdt>,
    /// Trials per workload and library.
    #[arg(long, default_value_t = 5)]
    trials: usize,
    /// Skips the extra trial that measures memory.
    #[arg(long)]
    no_memory: bool,
    /// Uses running replicas instead of adapters in this process. Give a
    /// comma-separated list of `host:port` values.
    #[arg(long, value_delimiter = ',')]
    replicas: Vec<String>,
    /// The result file. The default is standard output.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Lists the workloads and exits.
    #[arg(long)]
    list: bool,
}

/// Returns true if `--only` and `--include-heavy` select `workload`.
fn selected(workload: &Workload, args: &Args) -> bool {
    if workload.heavy && !args.include_heavy {
        return false;
    }
    args.only.is_empty()
        || args.only.iter().any(|name| {
            if name.ends_with('.') {
                workload.name.starts_with(name.as_str())
            } else {
                workload.name == *name
            }
        })
}

/// Why this lane cannot run `plan`, if it cannot.
fn skip_reason(plan: &Plan, lane: &Lane) -> Option<String> {
    if plan.trace.non_ascii && lane.crdt() == Crdt::Yrs {
        return Some(
            "the Yrs adapter indexes text by bytes, and this trace is not ASCII. \
             Use the ascii_only form of the trace."
                .to_owned(),
        );
    }
    if let Some(capacity) = lane.capacity()
        && plan.replica_count() > capacity
    {
        return Some(format!(
            "needs {} replicas, the lane has {capacity}",
            plan.replica_count()
        ));
    }
    None
}

/// Runs all trials of one plan on one lane. Returns false if any trial failed.
fn run_plan(plan: &Plan, lane: &Lane, args: &Args, out: &mut dyn Write) -> Result<bool> {
    let crdt = lane.crdt().as_str();
    let key = RowKey {
        workload: plan.name(),
        crdt,
        lane: lane.name(),
    };
    if let Some(reason) = skip_reason(plan, lane) {
        eprintln!("{} {crdt}: skipped: {reason}", plan.name());
        let reason = reason.as_str();
        write_row(out, &Row::Skipped { key, reason })?;
        return Ok(true);
    }
    let ids = Ids {
        order: plan.id_order(args.id_order),
        seed: args.id_seed,
    };
    for trial in 1..=args.trials {
        match run_trial(plan, lane, ids) {
            Ok(result) => {
                eprintln!(
                    "{} {crdt} trial {trial}: {:.1} ms, {} bytes",
                    plan.name(),
                    result.time_ms,
                    result.doc_size
                );
                write_row(out, &Row::Trial { key, trial, result })?;
            }
            Err(e) => {
                let error = format!("{e:#}");
                eprintln!("{} {crdt} trial {trial}: FAILED: {error}", plan.name());
                let trial = Some(trial);
                write_row(
                    out,
                    &Row::Failed {
                        key,
                        trial,
                        error: &error,
                    },
                )?;
                return Ok(false);
            }
        }
    }
    // The memory count covers this process only, so it has no meaning for a
    // remote replica.
    if !args.no_memory && matches!(lane, Lane::InProcess(_)) {
        match run_memory_trial(plan, lane, ids) {
            Ok(result) => write_row(out, &Row::Memory { key, result })?,
            Err(e) => {
                let error = format!("{e:#}");
                eprintln!("{} {crdt} memory trial: FAILED: {error}", plan.name());
                write_row(
                    out,
                    &Row::Failed {
                        key,
                        trial: None,
                        error: &error,
                    },
                )?;
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Writes one result row and flushes it, so a crash keeps the earlier rows.
fn write_row(out: &mut dyn Write, row: &impl Serialize) -> Result<()> {
    let line = serde_json::to_string(row).context("encoding a result row")?;
    writeln!(out, "{line}").context("writing a result row")?;
    out.flush().context("flushing the results")
}

/// Returns the plans to run. A workload that cannot load is an error, because a
/// missing workload must not look like a clean run.
fn load_plans(args: &Args) -> Result<Vec<Plan>> {
    if let Some(path) = &args.trace {
        let bytes = read_verified(path, None)?;
        let trace = Trace::parse(&bytes).with_context(|| path.display().to_string())?;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        return Ok(vec![Plan::new(
            Workload::ad_hoc(name, path.clone()),
            trace,
        )?]);
    }
    spec::load(&args.spec)?
        .into_iter()
        .filter(|w| selected(w, args))
        .map(|w| {
            let name = w.name.clone();
            let bytes = read_verified(&args.data_dir.join(&w.trace), Some(&w.sha256))
                .with_context(|| name.clone())?;
            let trace = Trace::parse(&bytes).with_context(|| name.clone())?;
            Plan::new(w, trace).with_context(|| name)
        })
        .collect()
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.list {
        for w in spec::load(&args.spec)? {
            let heavy = if w.heavy { " (heavy)" } else { "" };
            println!("{}{heavy}: {}", w.name, w.description);
        }
        return Ok(());
    }
    if !args.replicas.is_empty() && args.crdt.len() != 1 {
        bail!("with --replicas, give exactly one --crdt: the library that the replicas run");
    }
    let plans = load_plans(&args)?;
    if plans.is_empty() {
        bail!("no workload matches --only {:?}", args.only);
    }

    let mut out: Box<dyn Write> = match &args.out {
        Some(path) => {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let file =
                File::create(path).with_context(|| format!("creating {}", path.display()))?;
            Box::new(BufWriter::new(file))
        }
        None => Box::new(std::io::stdout()),
    };
    let run_args = json!({
        "spec": args.spec,
        "data_dir": args.data_dir,
        "trace": args.trace,
        "only": args.only,
        "crdt": args.crdt.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
        "trials": args.trials,
        "replicas": args.replicas,
        "id_order": args.id_order,
        "id_seed": args.id_seed,
    });
    write_row(&mut out, &output::run_row(&run_args))?;

    for plan in &plans {
        let row = Row::Workload {
            workload: plan.name(),
            agents: plan.trace.num_agents,
            id_order: plan.id_order(args.id_order),
            stats: plan.trace.graph_stats(),
        };
        write_row(&mut out, &row)?;
    }

    let mut clean = true;
    for &crdt in &args.crdt {
        let lane = if args.replicas.is_empty() {
            Lane::InProcess(crdt)
        } else {
            Lane::connect(crdt, &args.replicas)?
        };
        for plan in &plans {
            clean &= run_plan(plan, &lane, &args, &mut out)?;
        }
    }
    if !clean {
        bail!("at least one trial failed. See the failed rows in the results.");
    }
    Ok(())
}
