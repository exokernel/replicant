// Compiles ../../proto/replicant.proto into Rust client and server modules.
// The output is $OUT_DIR/replicant.v1.rs, which `common::proto` includes.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .compile_protos(&["../../proto/replicant.proto"], &["../../proto"])?;
    println!("cargo:rerun-if-changed=../../proto/replicant.proto");
    Ok(())
}
