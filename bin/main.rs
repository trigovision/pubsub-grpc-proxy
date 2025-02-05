use std::fmt::Debug;

use anyhow::Context;
use clap::Parser;

use pubsub_grpc_proxy::{
    auth,
    interceptors::{NamespaceInterceptor, PassthroughInterceptor, ProxyInterceptorVariant},
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

    #[arg(long)]
    cleanup_on_shutdown: bool,
}

fn create_interceptor(
    interceptor: &str,
    interceptor_arg: Option<String>,
) -> ProxyInterceptorVariant {
    match interceptor {
        "passthrough" => ProxyInterceptorVariant::Passthrough(PassthroughInterceptor::default()),
        "namespace" => ProxyInterceptorVariant::Namespace(NamespaceInterceptor::new(
            interceptor_arg.expect("Namespace interceptor requires an argument"),
        )),
        _ => panic!("Unknown interceptor type"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info,tonic::transport::server=trace");
    }

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
        args.cleanup_on_shutdown,
    )
    .await
    .map_err(|err| Into::<anyhow::Error>::into(err))?;

    // Run the proxy server
    let addr = format!("0.0.0.0:{}", args.port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(format!("Failed to bind to port: {}", args.port))?;

    // Listen for ctrl-c to gracefully terminate the server
    // Used to support cleanup of topics/subscriptions
    let ctrlc_signal = tokio::signal::ctrl_c();
    let (shutdown_send, shutdown_recv) = tokio::sync::mpsc::channel::<()>(1);

    // Convert ctrl-c signal into a oneshot signal
    let shutdown_sender_clone = shutdown_send.clone();
    tokio::spawn(async move {
        let _ = ctrlc_signal.await;
        let _ = shutdown_sender_clone.send(());
    });

    let server_handle =
        pubsub_grpc_proxy::run_server(proxy, listener, (shutdown_send, shutdown_recv)).await;

    server_handle
        .wait_for_completion()
        .await
        .map_err(|err| err.into())
}
