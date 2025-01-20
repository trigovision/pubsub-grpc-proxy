pub mod pubsub {
    #[cfg(any(feature = "use-precompiled-protos", feature = "precompile-protos"))]
    include!("google.pubsub.v1.rs");

    #[cfg(not(any(feature = "use-precompiled-protos", feature = "precompile-protos")))]
    tonic::include_proto!("google.pubsub.v1");
}
