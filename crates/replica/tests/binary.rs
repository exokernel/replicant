//! Tests of the `replica` binary as a process: its arguments, sync between two
//! processes, and a clean exit on SIGTERM.
//!
//! In the container lanes the binary is PID 1, and `docker compose down` sends
//! SIGTERM. The binary must then close its peer streams and exit, or the stop
//! waits for its timeout and the last metrics are lost.

use std::net::TcpListener;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use common::proto::{Empty, MapPut, OpRequest, PeerRef, ScalarValue, op_request, scalar_value};

/// The path of the binary that Cargo built for these tests.
const REPLICA: &str = env!("CARGO_BIN_EXE_replica");

/// A replica process. Dropping it kills the process, so a failed test leaves
/// no process behind.
struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Returns a loopback port that is free now.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Starts a replica with `crdt` and actor id `actor`, and waits until it
/// accepts gRPC calls. Returns the process and its address.
async fn start(actor: &str, crdt: &str) -> (Process, String) {
    let port = free_port();
    let child = Command::new(REPLICA)
        .args([
            "--actor",
            actor,
            "--crdt",
            crdt,
            "--port",
            &port.to_string(),
        ])
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let process = Process(child);
    let addr = format!("127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(20);
    while common::connect_replica(&addr).await.is_err() {
        assert!(Instant::now() < deadline, "{actor} did not start");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (process, addr)
}

/// Sends SIGTERM to `process`, and returns its exit status. Fails if it does
/// not exit within 10 s.
///
/// The wait must not block the runtime. A graceful shutdown waits for each
/// client connection to close, and the runtime of the test closes the test's
/// own connections.
async fn terminate(process: &mut Process) -> ExitStatus {
    let pid = process.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "the replica ignored SIGTERM");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Two processes sync over a real stream. Then each one exits with success on
/// SIGTERM, while the stream between them is still open.
#[cfg(unix)]
#[tokio::test]
async fn two_processes_sync_and_exit_cleanly_on_sigterm() {
    let (mut a, a_addr) = start("node-0", "yrs").await;
    let (mut b, b_addr) = start("node-1", "yrs").await;
    let mut a_client = common::connect_replica(&a_addr).await.unwrap();
    let mut b_client = common::connect_replica(&b_addr).await.unwrap();
    a_client
        .connect_peer(PeerRef {
            peer_id: "node-1".to_owned(),
            addr: b_addr,
        })
        .await
        .unwrap();
    a_client
        .apply_op(OpRequest {
            op: Some(op_request::Op::MapPut(MapPut {
                obj: String::new(),
                key: "k".to_owned(),
                value: Some(ScalarValue {
                    value: Some(scalar_value::Value::StrVal("v".to_owned())),
                }),
            })),
        })
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let fa = a_client.get_state_fingerprint(Empty {}).await.unwrap();
        let fb = b_client.get_state_fingerprint(Empty {}).await.unwrap();
        let (fa, fb) = (fa.into_inner().fingerprint, fb.into_inner().fingerprint);
        if !fa.is_empty() && fa == fb {
            break;
        }
        assert!(Instant::now() < deadline, "the processes did not converge");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // `a` opened the Sync stream, so stop `b` first: it must shut down while an
    // inbound stream is open. Then `a` stops after its peer went away.
    drop((a_client, b_client));
    assert!(terminate(&mut b).await.success());
    assert!(terminate(&mut a).await.success());
}

#[test]
fn an_unknown_library_is_refused_with_the_accepted_names() {
    let out = Command::new(REPLICA)
        .args(["--actor", "node-0", "--crdt", "nope"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    for name in ["automerge", "yrs", "loro"] {
        assert!(stderr.contains(name), "{stderr}");
    }
}

#[test]
fn an_actor_id_that_is_not_a_header_value_is_refused() {
    let out = Command::new(REPLICA)
        .args(["--actor", "node 0\n"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("header"));
}
