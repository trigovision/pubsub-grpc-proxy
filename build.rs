fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let google_apis = std::env::var("GOOGLE_APIS_DIR").unwrap_or(crate_root + "/proto");

    

    tonic_build::configure()
        .build_server(true)
        .compile_protos(&["google/pubsub/v1/pubsub.proto"], &[google_apis])?;
    Ok(())
}
