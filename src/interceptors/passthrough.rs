use super::ProxyInterceptor;
use crate::proto::pubsub::*;

#[derive(Clone, Default, Debug)]
pub struct PassthroughInterceptor;

impl ProxyInterceptor for PassthroughInterceptor {
    fn transform_create_topic(&self, request: Topic) -> Topic {
        request
    }
    fn transform_update_topic(&self, request: UpdateTopicRequest) -> UpdateTopicRequest {
        request
    }
    fn transform_publish(&self, request: PublishRequest) -> PublishRequest {
        request
    }
    fn transform_get_topic(&self, request: GetTopicRequest) -> GetTopicRequest {
        request
    }
    fn transform_list_topics(&self, request: ListTopicsRequest) -> ListTopicsRequest {
        request
    }
    fn transform_list_topic_subscriptions(
        &self,
        request: ListTopicSubscriptionsRequest,
    ) -> ListTopicSubscriptionsRequest {
        request
    }
    fn transform_list_topic_snapshots(
        &self,
        request: ListTopicSnapshotsRequest,
    ) -> ListTopicSnapshotsRequest {
        request
    }
    fn transform_delete_topic(&self, request: DeleteTopicRequest) -> DeleteTopicRequest {
        request
    }
    fn transform_get_subscription(
        &self,
        request: GetSubscriptionRequest,
    ) -> GetSubscriptionRequest {
        request
    }
    fn transform_list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> ListSubscriptionsRequest {
        request
    }
    fn transform_delete_subscription(
        &self,
        request: DeleteSubscriptionRequest,
    ) -> DeleteSubscriptionRequest {
        request
    }
    fn transform_modify_ack_deadline(
        &self,
        request: ModifyAckDeadlineRequest,
    ) -> ModifyAckDeadlineRequest {
        request
    }
    fn transform_pull(&self, request: PullRequest) -> PullRequest {
        request
    }
    fn transform_acknowledge(&self, request: AcknowledgeRequest) -> AcknowledgeRequest {
        request
    }
    fn transform_modify_push_config(
        &self,
        request: ModifyPushConfigRequest,
    ) -> ModifyPushConfigRequest {
        request
    }
    fn transform_detach_subscription(
        &self,
        request: DetachSubscriptionRequest,
    ) -> DetachSubscriptionRequest {
        request
    }
    fn transform_create_subscription(&self, request: Subscription) -> Subscription {
        request
    }
    fn transform_update_subscription(
        &self,
        request: UpdateSubscriptionRequest,
    ) -> UpdateSubscriptionRequest {
        request
    }
    fn transform_streaming_pull(&self, request: StreamingPullRequest) -> StreamingPullRequest {
        request
    }
    fn transform_get_snapshot(&self, request: GetSnapshotRequest) -> GetSnapshotRequest {
        request
    }
    fn transform_list_snapshots(&self, request: ListSnapshotsRequest) -> ListSnapshotsRequest {
        request
    }
    fn transform_create_snapshot(&self, request: CreateSnapshotRequest) -> CreateSnapshotRequest {
        request
    }
    fn transform_delete_snapshot(&self, request: DeleteSnapshotRequest) -> DeleteSnapshotRequest {
        request
    }
    fn transform_seek(&self, request: SeekRequest) -> SeekRequest {
        request
    }
    fn transform_update_snapshot(&self, request: UpdateSnapshotRequest) -> UpdateSnapshotRequest {
        request
    }
}
