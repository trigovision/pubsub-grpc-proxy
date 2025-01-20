fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    tonic_build::configure()
        .build_server(true)
        .compile_protos(
            &["google/pubsub/v1/pubsub.proto"],
            &[crate_root + "/proto"],
        )?;
    Ok(())
} 