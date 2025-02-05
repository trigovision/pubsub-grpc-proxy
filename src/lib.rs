pub mod auth;
pub mod interceptors;
pub mod proto;

mod cleanup;
mod tests;

use std::pin::Pin;
use std::time::Duration;

use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

use auth::TokenInterceptor;
use cleanup::PubsubCleanup;
use proto::pubsub::publisher_server::{Publisher, PublisherServer};
use proto::pubsub::subscriber_server::{Subscriber, SubscriberServer};
use proto::pubsub::*;

pub const CERTIFICATES: &[u8] = include_bytes!("../google-roots.pem");

pub type AuthenticatedPublisher =
    publisher_client::PublisherClient<InterceptedService<Channel, TokenInterceptor>>;

pub type AuthenticatedSubscriber =
    subscriber_client::SubscriberClient<InterceptedService<Channel, TokenInterceptor>>;

#[derive(Debug)]
pub enum PubSubProxyError {
    TransportError(tonic::transport::Error),
    AuthError(gouth::Error),
}

#[derive(Clone)]
pub struct PubSubProxy {
    publisher_client: AuthenticatedPublisher,
    subscriber_client: AuthenticatedSubscriber,
    interceptor: interceptors::ProxyInterceptorVariant,

    cleanup_requested: bool,
    cleanup_tool: PubsubCleanup,
}

impl PubSubProxy {
    pub async fn new(
        auth_method: auth::AuthMethod,
        interceptor: interceptors::ProxyInterceptorVariant,
        cleanup_on_shutdown: bool,
    ) -> Result<Self, PubSubProxyError> {
        let (publisher_client, subscriber_client) =
            connect_upstream_pubsub_googleapis(auth_method).await?;

        let cleanup_tool = PubsubCleanup::new(publisher_client.clone(), subscriber_client.clone());

        Ok(Self {
            publisher_client,
            subscriber_client,
            interceptor,
            cleanup_requested: cleanup_on_shutdown,
            cleanup_tool,
        })
    }

    pub async fn cleanup_all_topics_and_subscriptions_if_needed(&mut self) {
        if !self.cleanup_requested {
            return;
        }

        let max_parallel: usize = std::env::var("DELETE_MAX_PARALLEL")
            .unwrap_or_default()
            .parse()
            .unwrap_or(16);

        self.cleanup_tool
            .cleanup_all_topics_and_subscriptions(max_parallel)
            .await
    }

    pub fn build_service(&self) -> (PublisherServer<Self>, SubscriberServer<Self>) {
        (
            PublisherServer::new(self.clone()),
            SubscriberServer::new(self.clone()),
        )
    }
}

#[tonic::async_trait]
impl Publisher for PubSubProxy {
    async fn create_topic(&self, request: Request<Topic>) -> Result<Response<Topic>, Status> {
        tracing::info!("Proxied call to create_topic");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_create_topic(request.into_inner());

        tracing::debug!("Transformed request: {:?}", transformed);

        self.cleanup_tool.record_topic(transformed.name.clone());

        let response = self
            .publisher_client
            .clone()
            .create_topic(transformed)
            .await;

        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn update_topic(
        &self,
        request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        tracing::info!("Proxied call to update_topic");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_update_topic(request.into_inner());

        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .publisher_client
            .clone()
            .update_topic(transformed)
            .await;

        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        tracing::info!("Proxied call to publish");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_publish(request.into_inner());

        // Log the transformed request, without the `data` field to avoid printing binaries
        tracing::debug!(
            "Transformed request: {:?}",
            PublishRequest {
                messages: transformed
                    .messages
                    .iter()
                    .map(|msg| PubsubMessage {
                        data: vec![],
                        ..msg.clone()
                    })
                    .collect(),
                ..transformed.clone()
            }
        );

        let response = self.publisher_client.clone().publish(transformed).await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn get_topic(
        &self,
        request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        tracing::info!("Proxied call to get_topic");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_get_topic(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self.publisher_client.clone().get_topic(transformed).await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn list_topics(
        &self,
        request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        tracing::info!("Proxied call to list_topics");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_list_topics(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self.publisher_client.clone().list_topics(transformed).await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn list_topic_subscriptions(
        &self,
        request: Request<ListTopicSubscriptionsRequest>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        tracing::info!("Proxied call to list_topic_subscriptions");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_list_topic_subscriptions(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .publisher_client
            .clone()
            .list_topic_subscriptions(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn list_topic_snapshots(
        &self,
        request: Request<ListTopicSnapshotsRequest>,
    ) -> Result<Response<ListTopicSnapshotsResponse>, Status> {
        tracing::info!("Proxied call to list_topic_snapshots");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_list_topic_snapshots(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .publisher_client
            .clone()
            .list_topic_snapshots(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn delete_topic(
        &self,
        request: Request<DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to delete_topic");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_delete_topic(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .publisher_client
            .clone()
            .delete_topic(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn detach_subscription(
        &self,
        request: Request<DetachSubscriptionRequest>,
    ) -> Result<Response<DetachSubscriptionResponse>, Status> {
        tracing::info!("Proxied call to detach_subscription");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_detach_subscription(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .publisher_client
            .clone()
            .detach_subscription(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }
}

#[tonic::async_trait]
impl Subscriber for PubSubProxy {
    async fn create_subscription(
        &self,
        request: Request<Subscription>,
    ) -> Result<Response<Subscription>, Status> {
        tracing::info!("Proxied call to create_subscription");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_create_subscription(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        self.cleanup_tool
            .record_subscription(transformed.name.clone());

        let response = self
            .subscriber_client
            .clone()
            .create_subscription(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn get_subscription(
        &self,
        request: Request<GetSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        tracing::info!("Proxied call to get_subscription");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_get_subscription(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .get_subscription(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn update_subscription(
        &self,
        request: Request<UpdateSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        tracing::info!("Proxied call to update_subscription");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_update_subscription(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .update_subscription(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn list_subscriptions(
        &self,
        request: Request<ListSubscriptionsRequest>,
    ) -> Result<Response<ListSubscriptionsResponse>, Status> {
        tracing::info!("Proxied call to list_subscriptions");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_list_subscriptions(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .list_subscriptions(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn delete_subscription(
        &self,
        request: Request<DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to delete_subscription");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_delete_subscription(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .delete_subscription(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn modify_ack_deadline(
        &self,
        request: Request<ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to modify_ack_deadline");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_modify_ack_deadline(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .modify_ack_deadline(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to acknowledge");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_acknowledge(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .acknowledge(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        tracing::info!("Proxied call to pull");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_pull(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self.subscriber_client.clone().pull(transformed).await;

        // Log the response, without the `data` field to avoid printing binaries
        let resp_cleaned = response
            .as_ref()
            .map(|resp| resp.get_ref())
            .map(|resp| PullResponse {
                received_messages: resp
                    .received_messages
                    .iter()
                    .cloned()
                    .map(|msg| ReceivedMessage {
                        message: msg.message.map(|inner_msg| PubsubMessage {
                            data: vec![],
                            ..inner_msg
                        }),
                        ..msg
                    })
                    .collect(),
            });

        tracing::debug!("Received response: {:?}", resp_cleaned);

        response
    }

    type StreamingPullStream =
        Pin<Box<dyn Stream<Item = Result<StreamingPullResponse, Status>> + Send + 'static>>;

    async fn streaming_pull(
        &self,
        request: Request<Streaming<StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        let mut client = self.subscriber_client.clone();
        let interceptor = self.interceptor.clone();

        let input_stream = request
            .into_inner()
            .map_ok(move |request| {
                let transformed = interceptor.as_dyn().transform_streaming_pull(request);
                tracing::debug!("Transformed streaming request: {:?}", transformed);

                transformed
            })
            .map(|result| result.unwrap_or_else(|e| panic!("Stream error: {}", e)));

        let response = client.streaming_pull(Request::new(input_stream)).await;
        tracing::debug!("Received streaming response: {:?}", response);

        let output_stream = response?.into_inner();
        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn modify_push_config(
        &self,
        request: Request<ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to modify_push_config");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_modify_push_config(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .modify_push_config(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        tracing::info!("Proxied call to get_snapshot");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_get_snapshot(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .get_snapshot(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        tracing::info!("Proxied call to list_snapshots");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_list_snapshots(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .list_snapshots(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        tracing::info!("Proxied call to create_snapshot");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_create_snapshot(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .create_snapshot(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn update_snapshot(
        &self,
        request: Request<UpdateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        tracing::info!("Proxied call to update_snapshot");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_update_snapshot(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .update_snapshot(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to delete_snapshot");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_delete_snapshot(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self
            .subscriber_client
            .clone()
            .delete_snapshot(transformed)
            .await;
        tracing::debug!("Received response: {:?}", response);

        response
    }

    async fn seek(&self, request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        tracing::info!("Proxied call to seek");

        let transformed = self
            .interceptor
            .as_dyn()
            .transform_seek(request.into_inner());
        tracing::debug!("Transformed request: {:?}", transformed);

        let response = self.subscriber_client.clone().seek(transformed).await;
        tracing::debug!("Received response: {:?}", response);

        response
    }
}

async fn connect_upstream_pubsub_googleapis(
    auth_method: auth::AuthMethod,
) -> Result<(AuthenticatedPublisher, AuthenticatedSubscriber), PubSubProxyError> {
    tracing::debug!("Creating TLS config...");
    let tls_config = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(CERTIFICATES))
        .domain_name("pubsub.googleapis.com");

    tracing::info!("Establishing channel connection to pubsub.googleapis.com...");
    let channel = tonic::transport::Channel::from_static("https://pubsub.googleapis.com/")
        .connect_timeout(Duration::from_secs(30))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true)
        .tls_config(tls_config)
        .expect("static address");

    tracing::info!("Awaiting channel connection...");
    let connected_channel = channel
        .connect()
        .await
        .map_err(|err| PubSubProxyError::TransportError(err))?;

    tracing::info!("Channel connected successfully");

    let token_interceptor: TokenInterceptor = auth_method
        .try_into()
        .map_err(|err| PubSubProxyError::AuthError(err))?;

    let publisher_client = publisher_client::PublisherClient::with_interceptor(
        connected_channel.clone(),
        token_interceptor.clone(),
    );

    let subscriber_client = subscriber_client::SubscriberClient::with_interceptor(
        connected_channel,
        token_interceptor.clone(),
    );

    Ok((publisher_client, subscriber_client))
}

pub struct RunningProxyHandle {
    shutdown_signal: tokio::sync::mpsc::Sender<()>,
    server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl RunningProxyHandle {
    pub async fn wait_for_completion(self) -> Result<(), PubSubProxyError> {
        tracing::debug!("Waiting for server shutdown");
        let task_result = self.server_task.await;

        tracing::info!("Server shutdown confirmed");

        task_result
            .expect("Task Join should work")
            .map_err(|err| PubSubProxyError::TransportError(err))
    }

    pub async fn shutdown(self) -> Result<(), PubSubProxyError> {
        tracing::debug!("Sending shutdown signal");
        let _ = self.shutdown_signal.send(()).await;

        tracing::debug!("Waiting for server shutdown");
        self.wait_for_completion().await
    }
}

pub async fn run_server(
    mut proxy: PubSubProxy,
    tcp_listener: TcpListener,
    shutdown_signal: (
        tokio::sync::mpsc::Sender<()>,
        tokio::sync::mpsc::Receiver<()>,
    ),
) -> RunningProxyHandle {
    let (publisher, subscriber) = proxy.build_service();

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<PublisherServer<PubSubProxy>>()
        .await;

    health_reporter
        .set_serving::<SubscriberServer<PubSubProxy>>()
        .await;

    tracing::info!(
        "PubSub proxy server listening on {}",
        tcp_listener.local_addr().unwrap()
    );

    let tcp_incoming =
        TcpIncoming::from_listener(tcp_listener, false, Some(Duration::from_secs(15)))
            .expect("TcpIncoming from TcpListener");

    let builder = tonic::transport::Server::builder()
        // Enable HTTP/1.1 support for local health check using curl
        .accept_http1(true)
        .trace_fn(|req| {
            let uri = req
                .uri()
                .path_and_query()
                .map(|p| p.to_string())
                .unwrap_or_default();

            tracing::info_span!("", uri)
        })
        .add_service(publisher.accept_compressed(CompressionEncoding::Gzip))
        .add_service(subscriber.accept_compressed(CompressionEncoding::Gzip))
        .add_service(health_service);

    let mut inner_shutdown_signal = shutdown_signal.1;
    let task = tokio::spawn(async move {
        let serve_result = builder
            .serve_with_incoming_shutdown(tcp_incoming, inner_shutdown_signal.recv().map(|_| ()))
            .await;

        proxy.cleanup_all_topics_and_subscriptions_if_needed().await;

        serve_result
    });

    RunningProxyHandle {
        shutdown_signal: shutdown_signal.0,
        server_task: task,
    }
}

impl Into<anyhow::Error> for PubSubProxyError {
    fn into(self) -> anyhow::Error {
        match self {
            PubSubProxyError::TransportError(error) => anyhow::anyhow!(error),
            PubSubProxyError::AuthError(error) => anyhow::anyhow!(error),
        }
    }
}
