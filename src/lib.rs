pub mod auth;
pub mod interceptors;
pub mod proto;

use auth::TokenInterceptor;
use futures::{Stream, StreamExt, TryStreamExt};
use proto::pubsub::publisher_server::{Publisher, PublisherServer};
use proto::pubsub::subscriber_server::{Subscriber, SubscriberServer};
use proto::pubsub::*;
use std::pin::Pin;
use std::time::Duration;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

pub const CERTIFICATES: &[u8] = include_bytes!("../google-roots.pem");

#[derive(Clone)]
pub struct PubSubProxy<T: interceptors::ProxyInterceptor + Clone> {
    publisher_client:
        publisher_client::PublisherClient<InterceptedService<Channel, TokenInterceptor>>,
    subscriber_client:
        subscriber_client::SubscriberClient<InterceptedService<Channel, TokenInterceptor>>,
    interceptor: T,
}

impl<T: interceptors::ProxyInterceptor + Clone> PubSubProxy<T> {
    pub async fn new(
        auth_method: auth::AuthMethod,
        interceptor: T,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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
        let connected_channel = channel.connect().await?;
        tracing::info!("Channel connected successfully");

        let token_interceptor: TokenInterceptor = auth_method.try_into()?;

        Ok(Self {
            publisher_client: publisher_client::PublisherClient::with_interceptor(
                connected_channel.clone(),
                token_interceptor.clone(),
            ),
            subscriber_client: subscriber_client::SubscriberClient::with_interceptor(
                connected_channel,
                token_interceptor.clone(),
            ),
            interceptor: interceptor,
        })
    }

    pub fn into_service(self) -> (PublisherServer<Self>, SubscriberServer<Self>) {
        (
            PublisherServer::new(self.clone()),
            SubscriberServer::new(self),
        )
    }
}

#[tonic::async_trait]
impl<T: interceptors::ProxyInterceptor + Clone> Publisher for PubSubProxy<T> {
    async fn create_topic(&self, request: Request<Topic>) -> Result<Response<Topic>, Status> {
        tracing::info!("Proxied call to create_topic");
        let transformed = self
            .interceptor
            .transform_create_topic(request.into_inner());
        self.publisher_client
            .clone()
            .create_topic(transformed)
            .await
    }

    async fn update_topic(
        &self,
        request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        tracing::info!("Proxied call to update_topic");
        let transformed = self
            .interceptor
            .transform_update_topic(request.into_inner());
        self.publisher_client
            .clone()
            .update_topic(transformed)
            .await
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        tracing::info!("Proxied call to publish");
        let transformed = self.interceptor.transform_publish(request.into_inner());
        self.publisher_client.clone().publish(transformed).await
    }

    async fn get_topic(
        &self,
        request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        tracing::info!("Proxied call to get_topic");
        let transformed = self.interceptor.transform_get_topic(request.into_inner());
        self.publisher_client.clone().get_topic(transformed).await
    }

    async fn list_topics(
        &self,
        request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        tracing::info!("Proxied call to list_topics");
        let transformed = self.interceptor.transform_list_topics(request.into_inner());
        self.publisher_client.clone().list_topics(transformed).await
    }

    async fn list_topic_subscriptions(
        &self,
        request: Request<ListTopicSubscriptionsRequest>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        tracing::info!("Proxied call to list_topic_subscriptions");
        let transformed = self
            .interceptor
            .transform_list_topic_subscriptions(request.into_inner());
        self.publisher_client
            .clone()
            .list_topic_subscriptions(transformed)
            .await
    }

    async fn list_topic_snapshots(
        &self,
        request: Request<ListTopicSnapshotsRequest>,
    ) -> Result<Response<ListTopicSnapshotsResponse>, Status> {
        tracing::info!("Proxied call to list_topic_snapshots");
        let transformed = self
            .interceptor
            .transform_list_topic_snapshots(request.into_inner());
        self.publisher_client
            .clone()
            .list_topic_snapshots(transformed)
            .await
    }

    async fn delete_topic(
        &self,
        request: Request<DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to delete_topic");
        let transformed = self
            .interceptor
            .transform_delete_topic(request.into_inner());
        self.publisher_client
            .clone()
            .delete_topic(transformed)
            .await
    }

    async fn detach_subscription(
        &self,
        request: Request<DetachSubscriptionRequest>,
    ) -> Result<Response<DetachSubscriptionResponse>, Status> {
        tracing::info!("Proxied call to detach_subscription");
        let transformed = self
            .interceptor
            .transform_detach_subscription(request.into_inner());
        self.publisher_client
            .clone()
            .detach_subscription(transformed)
            .await
    }
}

#[tonic::async_trait]
impl<T: interceptors::ProxyInterceptor + Clone> Subscriber for PubSubProxy<T> {
    async fn create_subscription(
        &self,
        request: Request<Subscription>,
    ) -> Result<Response<Subscription>, Status> {
        tracing::info!("Proxied call to create_subscription");
        let transformed = self
            .interceptor
            .transform_create_subscription(request.into_inner());
        self.subscriber_client
            .clone()
            .create_subscription(transformed)
            .await
    }

    async fn get_subscription(
        &self,
        request: Request<GetSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        tracing::info!("Proxied call to get_subscription");
        let transformed = self
            .interceptor
            .transform_get_subscription(request.into_inner());
        self.subscriber_client
            .clone()
            .get_subscription(transformed)
            .await
    }

    async fn update_subscription(
        &self,
        request: Request<UpdateSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        tracing::info!("Proxied call to update_subscription");
        let transformed = self
            .interceptor
            .transform_update_subscription(request.into_inner());
        self.subscriber_client
            .clone()
            .update_subscription(transformed)
            .await
    }

    async fn list_subscriptions(
        &self,
        request: Request<ListSubscriptionsRequest>,
    ) -> Result<Response<ListSubscriptionsResponse>, Status> {
        tracing::info!("Proxied call to list_subscriptions");
        let transformed = self
            .interceptor
            .transform_list_subscriptions(request.into_inner());
        self.subscriber_client
            .clone()
            .list_subscriptions(transformed)
            .await
    }

    async fn delete_subscription(
        &self,
        request: Request<DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to delete_subscription");
        let transformed = self
            .interceptor
            .transform_delete_subscription(request.into_inner());
        self.subscriber_client
            .clone()
            .delete_subscription(transformed)
            .await
    }

    async fn modify_ack_deadline(
        &self,
        request: Request<ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to modify_ack_deadline");
        let transformed = self
            .interceptor
            .transform_modify_ack_deadline(request.into_inner());
        self.subscriber_client
            .clone()
            .modify_ack_deadline(transformed)
            .await
    }

    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to acknowledge");
        let transformed = self.interceptor.transform_acknowledge(request.into_inner());
        self.subscriber_client
            .clone()
            .acknowledge(transformed)
            .await
    }

    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        tracing::info!("Proxied call to pull");
        let transformed = self.interceptor.transform_pull(request.into_inner());
        self.subscriber_client.clone().pull(transformed).await
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
            .map_ok(move |request| interceptor.clone().transform_streaming_pull(request))
            .map(|result| result.unwrap_or_else(|e| panic!("Stream error: {}", e)));

        let output_stream = client
            .streaming_pull(Request::new(input_stream))
            .await?
            .into_inner();
        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn modify_push_config(
        &self,
        request: Request<ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to modify_push_config");
        let transformed = self
            .interceptor
            .transform_modify_push_config(request.into_inner());
        self.subscriber_client
            .clone()
            .modify_push_config(transformed)
            .await
    }

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        tracing::info!("Proxied call to get_snapshot");
        let transformed = self
            .interceptor
            .transform_get_snapshot(request.into_inner());
        self.subscriber_client
            .clone()
            .get_snapshot(transformed)
            .await
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        tracing::info!("Proxied call to list_snapshots");
        let transformed = self
            .interceptor
            .transform_list_snapshots(request.into_inner());
        self.subscriber_client
            .clone()
            .list_snapshots(transformed)
            .await
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        tracing::info!("Proxied call to create_snapshot");
        let transformed = self
            .interceptor
            .transform_create_snapshot(request.into_inner());
        self.subscriber_client
            .clone()
            .create_snapshot(transformed)
            .await
    }

    async fn update_snapshot(
        &self,
        request: Request<UpdateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        tracing::info!("Proxied call to update_snapshot");
        let transformed = self
            .interceptor
            .transform_update_snapshot(request.into_inner());
        self.subscriber_client
            .clone()
            .update_snapshot(transformed)
            .await
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        tracing::info!("Proxied call to delete_snapshot");
        let transformed = self
            .interceptor
            .transform_delete_snapshot(request.into_inner());
        self.subscriber_client
            .clone()
            .delete_snapshot(transformed)
            .await
    }

    async fn seek(&self, request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        tracing::info!("Proxied call to seek");
        let transformed = self.interceptor.transform_seek(request.into_inner());
        self.subscriber_client.clone().seek(transformed).await
    }
}

pub async fn run_server<T: interceptors::ProxyInterceptor + Clone>(
    proxy: PubSubProxy<T>,
    bind_addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = bind_addr.parse()?;
    let (publisher, subscriber) = proxy.into_service();

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<PublisherServer<PubSubProxy<T>>>()
        .await;

    health_reporter
        .set_serving::<SubscriberServer<PubSubProxy<T>>>()
        .await;

    tracing::info!("PubSub proxy server listening on {}", addr);

    let res = tonic::transport::Server::builder()
        // Enable HTTP/1.1 support for local health check using curl
        .accept_http1(true)
        .add_service(publisher.accept_compressed(CompressionEncoding::Gzip))
        .add_service(subscriber.accept_compressed(CompressionEncoding::Gzip))
        .add_service(health_service)
        .serve(addr)
        .await;

    if let Err(e) = res {
        panic!("Server error: {}", e);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use interceptors::PassthroughInterceptor;

    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_pubsub_proxy() {
        std::env::set_var("RUST_LOG", "pubsub_grpc_proxy=debug,info");
        tracing_subscriber::fmt::init();

        let project_id =
            std::env::var("GCP_PROJECT_ID").expect("Assuming GCP_PROJECT_ID variable is present");

        // Start the proxy server
        let proxy = PubSubProxy::new(
            super::auth::AuthMethod::ApplicationDefaultCredentials(project_id.clone()),
            PassthroughInterceptor::default(),
        )
        .await
        .unwrap();

        let proxy_addr = "0.0.0.0:1234";
        let server = run_server(proxy, proxy_addr);
        let _server_handle = tokio::spawn(async move {
            match server.await {
                Ok(_) => (),
                Err(e) => {
                    panic!("Server error: {}", e);
                }
            }
        });

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Initialize client pointing to our proxy
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", proxy_addr))
            .unwrap()
            .connect()
            .await
            .unwrap();

        let mut publisher = publisher_client::PublisherClient::new(channel.clone());
        let mut subscriber = subscriber_client::SubscriberClient::new(channel);

        // Create a topic
        let topic_path = format!("projects/{}/topics/test-topic-2", &project_id);
        let topic = Topic {
            name: topic_path.clone(),
            labels: Default::default(),
            message_storage_policy: None,
            kms_key_name: "".to_string(),
            schema_settings: None,
            satisfies_pzs: false,
            message_retention_duration: None,
            ingestion_data_source_settings: None,
            state: i32::default(),
        };
        let topic = publisher
            .create_topic(Request::new(topic))
            .await
            .unwrap()
            .into_inner();
        let topic_name = topic.name.clone();

        // Create a subscription
        let sub_path = format!("projects/{}/subscriptions/test-sub-2", &project_id);
        let subscription = Subscription {
            name: sub_path,
            topic: topic_name,
            push_config: None,
            ack_deadline_seconds: 10,
            retain_acked_messages: false,
            message_retention_duration: None,
            labels: Default::default(),
            enable_message_ordering: false,
            expiration_policy: None,
            filter: "".to_string(),
            dead_letter_policy: None,
            retry_policy: None,
            detached: false,
            enable_exactly_once_delivery: false,
            topic_message_retention_duration: None,
            state: i32::default(),
            analytics_hub_subscription_info: None,
            bigquery_config: None,
            cloud_storage_config: None,
        };
        let subscription = subscriber
            .create_subscription(Request::new(subscription))
            .await
            .unwrap()
            .into_inner();
        let subscription_name = subscription.name.clone();

        // Publish a message
        let publish_request = PublishRequest {
            topic: topic.name,
            messages: vec![PubsubMessage {
                data: b"Hello, world!".to_vec(),
                attributes: Default::default(),
                message_id: "".to_string(),
                publish_time: None,
                ordering_key: "".to_string(),
            }],
        };
        let publish_response = publisher
            .publish(Request::new(publish_request))
            .await
            .unwrap();
        println!("Published message ID: {:?}", publish_response);

        // Pull the message
        let pull_request = PullRequest {
            subscription: subscription_name.clone(),
            max_messages: 1,
            ..Default::default()
        };
        let pull_response = subscriber
            .pull(Request::new(pull_request))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(pull_response.received_messages.len(), 1);
        assert_eq!(
            pull_response.received_messages[0]
                .message
                .as_ref()
                .unwrap()
                .data,
            b"Hello, world!"
        );

        // Acknowledge the message
        let ack_request = AcknowledgeRequest {
            subscription: subscription_name,
            ack_ids: pull_response
                .received_messages
                .iter()
                .map(|msg| msg.ack_id.clone())
                .collect(),
        };
        subscriber
            .acknowledge(Request::new(ack_request))
            .await
            .unwrap();
    }
}
