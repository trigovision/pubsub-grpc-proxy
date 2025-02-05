#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;
    use tonic::transport::Channel;

    use crate::auth::AuthMethod;
    use crate::interceptors::{PassthroughInterceptor, ProxyInterceptorVariant};
    use crate::publisher_client::PublisherClient;
    use crate::subscriber_client::SubscriberClient;
    use crate::{connect_upstream_pubsub_googleapis, run_server, PubSubProxy, RunningProxyHandle};

    use crate::proto::pubsub::*;

    use std::time::Duration;

    const RUST_LOG: &'static str = "info,pubsub_grpc_proxy=debug,tonic::transport::server=trace";

    #[rstest::fixture]
    fn gcp_project_id() -> String {
        std::env::var("GCP_PROJECT_ID").expect("Assuming GCP_PROJECT_ID variable is present")
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_delete_on_exit(gcp_project_id: String) {
        std::env::set_var("RUST_LOG", RUST_LOG);
        let _ = tracing_subscriber::fmt::try_init();

        // Start the proxy server
        let proxy = build_passthrough_proxy(&gcp_project_id).await;
        let (signal_send, signal_recv) = tokio::sync::mpsc::channel::<()>(1);
        let (server_handle, mut publisher, _) =
            start_server_attached(proxy, (signal_send, signal_recv)).await;

        // Create a new topic through the proxy
        let topic_name = _test_topic_name(&gcp_project_id, "test_delete_on_exit");

        let create_result: anyhow::Result<()> = async {
            let _ = publisher
                .create_topic(tonic::Request::new(Topic {
                    name: topic_name.clone(),
                    ..Default::default()
                }))
                .await?;

            Ok(())
        }
        .await;

        // Shutdown the proxy before testing for topic deletion
        drop(publisher);
        let _ = server_handle.shutdown().await;

        assert!(
            create_result.is_ok(),
            "Failed to create test topic name: {}",
            create_result.unwrap_err()
        );

        // Test if topic exists on pubsub
        let (mut publisher, _) = connect_upstream_pubsub_googleapis(
            AuthMethod::ApplicationDefaultCredentials(gcp_project_id.clone()),
        )
        .await
        .expect("Upstream connection to pubsub");

        let resp = publisher
            .get_topic(tonic::Request::new(GetTopicRequest { topic: topic_name }))
            .await;

        assert!(resp.is_err(), "Expected to receive NOT_FOUND error");
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_pubsub_proxy_sanity(gcp_project_id: String) {
        std::env::set_var("RUST_LOG", RUST_LOG);
        let _ = tracing_subscriber::fmt::try_init();

        // Start the proxy server
        let proxy = build_passthrough_proxy(&gcp_project_id).await;
        let (signal_send, signal_recv) = tokio::sync::mpsc::channel::<()>(1);

        let (server_handle, mut publisher, mut subscriber) =
            start_server_attached(proxy, (signal_send, signal_recv)).await;

        let test_result: anyhow::Result<()> = async move {
            // Create a topic
            let topic_path = _test_topic_name(&gcp_project_id, "test_pubsub_proxy_sanity");
            let topic = Topic {
                name: topic_path.clone(),
                ..Default::default()
            };

            let topic = publisher
                .create_topic(tonic::Request::new(topic))
                .await?
                .into_inner();
            let topic_name = topic.name.clone();

            // Create a subscription
            let sub_path = _test_subscription_name(&gcp_project_id, "test_pubsub_proxy_sanity");
            let subscription = Subscription {
                name: sub_path,
                topic: topic_name,
                ..Default::default()
            };

            let subscription = subscriber
                .create_subscription(tonic::Request::new(subscription))
                .await?
                .into_inner();
            let subscription_name = subscription.name.clone();

            // Publish a message
            let publish_request = PublishRequest {
                topic: topic.name,
                messages: vec![PubsubMessage {
                    data: b"Hello, world!".to_vec(),
                    ..Default::default()
                }],
            };
            let publish_response = publisher
                .publish(tonic::Request::new(publish_request))
                .await?;
            println!("Published message ID: {:?}", publish_response);

            // Pull the message
            let pull_request = PullRequest {
                subscription: subscription_name.clone(),
                max_messages: 1,
                ..Default::default()
            };
            let pull_response = subscriber
                .pull(tonic::Request::new(pull_request))
                .await?
                .into_inner();

            assert_eq!(pull_response.received_messages.len(), 1);
            assert_eq!(
                pull_response.received_messages[0]
                    .message
                    .as_ref()
                    .ok_or(anyhow::anyhow!("msg exists"))?
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
                .acknowledge(tonic::Request::new(ack_request))
                .await?;

            Ok(())
        }
        .await;

        // Always gracefully shutdown the server
        server_handle.shutdown().await.expect("Server shutdown");

        assert!(
            test_result.is_ok(),
            "Failed due to: {}",
            test_result.unwrap_err()
        );
    }

    async fn build_passthrough_proxy(gcp_project_id: &str) -> PubSubProxy {
        PubSubProxy::new(
            AuthMethod::ApplicationDefaultCredentials(gcp_project_id.to_owned()),
            ProxyInterceptorVariant::Passthrough(PassthroughInterceptor::default()),
            // always cleanup_on_shutdown=true
            true,
        )
        .await
        .unwrap()
    }

    async fn start_server_attached(
        proxy: PubSubProxy,
        shutdown_signal: (
            tokio::sync::mpsc::Sender<()>,
            tokio::sync::mpsc::Receiver<()>,
        ),
    ) -> (
        RunningProxyHandle,
        PublisherClient<Channel>,
        SubscriberClient<Channel>,
    ) {
        // Use :0 to auto-bind to a local free port
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let local_bound_port = listener.local_addr().unwrap().port();

        let server = run_server(proxy, listener, shutdown_signal).await;

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Initialize client pointing to our proxy
        let channel = tonic::transport::Channel::from_shared(format!(
            "http://localhost:{}",
            local_bound_port
        ))
        .unwrap()
        .connect()
        .await
        .unwrap();

        let publisher = publisher_client::PublisherClient::new(channel.clone());
        let subscriber = subscriber_client::SubscriberClient::new(channel);
        return (server, publisher, subscriber);
    }

    fn _test_topic_name(gcp_project_id: &str, test_name: &str) -> String {
        let short_uuid = uuid::Uuid::new_v4().to_string()[..8].to_owned();

        format!(
            "projects/{}/topics/{}-{}",
            gcp_project_id, test_name, short_uuid
        )
    }

    fn _test_subscription_name(gcp_project_id: &str, test_name: &str) -> String {
        let short_uuid = uuid::Uuid::new_v4().to_string()[..8].to_owned();

        format!(
            "projects/{}/subscriptions/{}-{}",
            gcp_project_id, test_name, short_uuid
        )
    }
}
