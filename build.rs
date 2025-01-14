fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile_protos(
            &["proto/google/pubsub/v1/pubsub.proto"],
            &["proto"],
        )?;
    Ok(())
} 