use crate::proto::pubsub::*;

mod namespace;
mod passthrough;

pub use namespace::NamespaceInterceptor;
pub use passthrough::PassthroughInterceptor;

#[derive(Clone, Debug)]
pub enum ProxyInterceptorVariant {
    Passthrough(PassthroughInterceptor),
    Namespace(NamespaceInterceptor),
}

pub trait ProxyInterceptor: std::fmt::Debug + Send + Sync + 'static {
    fn transform_create_topic(&self, request: Topic) -> Topic;
    fn transform_update_topic(&self, request: UpdateTopicRequest) -> UpdateTopicRequest;
    fn transform_publish(&self, request: PublishRequest) -> PublishRequest;
    fn transform_get_topic(&self, request: GetTopicRequest) -> GetTopicRequest;
    fn transform_list_topics(&self, request: ListTopicsRequest) -> ListTopicsRequest;
    fn transform_list_topic_subscriptions(
        &self,
        request: ListTopicSubscriptionsRequest,
    ) -> ListTopicSubscriptionsRequest;

    fn transform_list_topic_snapshots(
        &self,
        request: ListTopicSnapshotsRequest,
    ) -> ListTopicSnapshotsRequest;
    fn transform_delete_topic(&self, request: DeleteTopicRequest) -> DeleteTopicRequest;
    fn transform_get_subscription(&self, request: GetSubscriptionRequest)
        -> GetSubscriptionRequest;
    fn transform_list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> ListSubscriptionsRequest;
    fn transform_delete_subscription(
        &self,
        request: DeleteSubscriptionRequest,
    ) -> DeleteSubscriptionRequest;
    fn transform_modify_ack_deadline(
        &self,
        request: ModifyAckDeadlineRequest,
    ) -> ModifyAckDeadlineRequest;
    fn transform_pull(&self, request: PullRequest) -> PullRequest;
    fn transform_acknowledge(&self, request: AcknowledgeRequest) -> AcknowledgeRequest;
    fn transform_modify_push_config(
        &self,
        request: ModifyPushConfigRequest,
    ) -> ModifyPushConfigRequest;
    fn transform_detach_subscription(
        &self,
        request: DetachSubscriptionRequest,
    ) -> DetachSubscriptionRequest;
    fn transform_create_subscription(&self, request: Subscription) -> Subscription;
    fn transform_update_subscription(
        &self,
        request: UpdateSubscriptionRequest,
    ) -> UpdateSubscriptionRequest;
    fn transform_streaming_pull(&self, request: StreamingPullRequest) -> StreamingPullRequest;
    fn transform_get_snapshot(&self, request: GetSnapshotRequest) -> GetSnapshotRequest;
    fn transform_list_snapshots(&self, request: ListSnapshotsRequest) -> ListSnapshotsRequest;
    fn transform_create_snapshot(&self, request: CreateSnapshotRequest) -> CreateSnapshotRequest;
    fn transform_delete_snapshot(&self, request: DeleteSnapshotRequest) -> DeleteSnapshotRequest;
    fn transform_seek(&self, request: SeekRequest) -> SeekRequest;
    fn transform_update_snapshot(&self, request: UpdateSnapshotRequest) -> UpdateSnapshotRequest;
}

impl ProxyInterceptorVariant {
    pub fn as_dyn(&self) -> &dyn ProxyInterceptor {
        match self {
            ProxyInterceptorVariant::Passthrough(passthrough_interceptor) => {
                passthrough_interceptor
            }
            ProxyInterceptorVariant::Namespace(namespace_interceptor) => namespace_interceptor,
        }
    }
}
