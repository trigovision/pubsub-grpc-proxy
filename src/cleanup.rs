use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use futures::StreamExt;

use crate::{proto::pubsub::*, AuthenticatedPublisher, AuthenticatedSubscriber};

#[derive(Clone)]
pub struct PubsubCleanup {
    // Authenticated gRPC client to pubsub for deleting the topics/subscriptions
    _subscriber_client: AuthenticatedSubscriber,
    _publisher_client: AuthenticatedPublisher,

    // Populated by `record_topic` and `record_subscription`
    _subscriptions_created: Arc<Mutex<HashSet<String>>>,
    _topics_created: Arc<Mutex<HashSet<String>>>,
}

impl PubsubCleanup {
    pub fn new(publisher: AuthenticatedPublisher, subscriber: AuthenticatedSubscriber) -> Self {
        Self {
            _publisher_client: publisher,
            _subscriber_client: subscriber,
            _subscriptions_created: Arc::new(Mutex::new(HashSet::new())),
            _topics_created: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn record_topic(&self, topic_name: String) {
        if let Ok(mut topics_created) = self._topics_created.lock() {
            topics_created.insert(topic_name);
        }
    }

    pub fn record_subscription(&self, subscrition_name: String) {
        if let Ok(mut subscriptions_created) = self._subscriptions_created.lock() {
            subscriptions_created.insert(subscrition_name);
        }
    }

    pub async fn cleanup_all_topics_and_subscriptions(&mut self, max_parallel: usize) {
        tracing::trace!("Cleaning up all subscriptions and topics");

        // Process subscriptions first
        self.cleanup_subscriptions(max_parallel).await;

        // Then process topics
        self.cleanup_topics(max_parallel).await;
    }

    async fn cleanup_subscriptions(&mut self, max_parallel: usize) {
        let subscriber_client = self._subscriber_client.clone();

        let subscriptions = self.drain_recorded_subscriptions();
        if subscriptions.is_empty() {
            return;
        }

        tracing::info!("Deleting {} subscriptions", subscriptions.len());

        let start = std::time::Instant::now();
        let _ = futures::stream::iter(subscriptions)
            .map(move |subscription| {
                let mut client_clone = subscriber_client.clone();

                async move {
                    client_clone
                        .delete_subscription(tonic::Request::new(DeleteSubscriptionRequest {
                            subscription,
                        }))
                        .await
                }
            })
            .buffer_unordered(max_parallel)
            .collect::<Vec<_>>()
            .await;

        tracing::info!(
            "Deleting subscriptions took {} seconds",
            start.elapsed().as_secs()
        );
    }

    async fn cleanup_topics(&mut self, max_parallel: usize) {
        let publisher_client = self._publisher_client.clone();

        let topics = self.drain_recorded_topics();
        if topics.is_empty() {
            return;
        }

        tracing::info!("Deleting {} topics", topics.len());

        let start = std::time::Instant::now();
        futures::stream::iter(topics)
            .map(move |topic| {
                let mut client_clone = publisher_client.clone();

                async move {
                    client_clone
                        .delete_topic(tonic::Request::new(DeleteTopicRequest { topic }))
                        .await
                }
            })
            .buffer_unordered(max_parallel)
            .collect::<Vec<_>>()
            .await;

        tracing::info!("Deleting topics took {} seconds", start.elapsed().as_secs());
    }

    fn drain_recorded_subscriptions(&mut self) -> Vec<String> {
        let mut subscriptions = self._subscriptions_created.lock().expect("Mutex lock");
        subscriptions.drain().collect()
    }

    fn drain_recorded_topics(&mut self) -> Vec<String> {
        let mut topics = self._topics_created.lock().expect("Mutex lock");
        topics.drain().collect()
    }
}

// impl Drop for PubsubCleanup {
//     fn drop(&mut self) {
//         tracing::info!("Deleting subscriptions and topics created during server run!");

//         let subscriptions = self.drain_recorded_subscriptions();
//         let topics = self.drain_recorded_topics();
//         let clients = (
//             self._publisher_client.clone(),
//             self._subscriber_client.clone(),
//         );

//         let max_parallel: usize = std::env::var("DELETE_MAX_PARALLEL")
//             .unwrap_or_default()
//             .parse()
//             .unwrap_or(16);

//         tokio::runtime::Handle::current().spawn(cleanup_all_topics_and_subscriptions(
//             subscriptions,
//             topics,
//             clients,
//             max_parallel,
//         ));
//     }
// }
