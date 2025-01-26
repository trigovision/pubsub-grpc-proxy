use std::{fmt::Debug, sync::Arc};

use clap::Parser;
use pubsub_grpc_proxy::{
    auth,
    interceptors::{namespace::NamespaceInterceptor, PassthroughInterceptor, ProxyInterceptor},
    PubSubProxy,
};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long, required = true)]
    project_id: String,

    #[arg(long, required = true)]
    port: u16,

    #[arg(long, required = true, value_parser(["passthrough", "namespace"]))]
    interceptor: String,

    #[arg(long)]
    interceptor_arg: Option<String>,
}

fn create_interceptor(
    interceptor: &str,
    interceptor_arg: Option<String>,
) -> Arc<dyn ProxyInterceptor> {
    match interceptor {
        "passthrough" => Arc::new(PassthroughInterceptor::default()),
        "namespace" => Arc::new(NamespaceInterceptor::new(
            interceptor_arg.expect("Namespace interceptor requires an argument"),
        )),
        _ => panic!("Unknown interceptor type"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    tracing::info!("Running with args: {:#?}", &args);

    // Create interceptor based on argument
    let interceptor = create_interceptor(&args.interceptor, args.interceptor_arg.clone());

    tracing::info!("Running with Interceptor: {:?}", &interceptor);

    // Create the proxy pointing to the actual PubSub service
    let proxy = PubSubProxy::new(
        auth::AuthMethod::ApplicationDefaultCredentials(args.project_id),
        interceptor,
    )
    .await?;

    // Run the proxy server
    let addr = format!("0.0.0.0:{}", args.port);
    pubsub_grpc_proxy::run_server(proxy, &addr).await?;

    Ok(())
}
