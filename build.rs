fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(feature = "use-precompiled-protos"))]
    {
        let mut builder = tonic_build::configure().build_server(true);

        if cfg!(feature = "precompile-protos") {
            builder = builder.out_dir("src/proto");
        }

        builder.compile_protos(&["google/pubsub/v1/pubsub.proto"], &["proto"])?;
    }
    Ok(())
}
