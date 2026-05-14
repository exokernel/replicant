// Compiles ../../proto/replicant.proto into Rust client/server modules
// emitted to $OUT_DIR/replicant.v1.rs, included by `common::proto`.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../../proto/replicant.proto"], &["../../proto"])?;
    println!("cargo:rerun-if-changed=../../proto/replicant.proto");
    Ok(())
}
