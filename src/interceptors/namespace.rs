use super::ProxyInterceptor;

#[derive(Clone)]
pub struct NamespaceInterceptor {
    pub prefix: String,
}

impl NamespaceInterceptor {
    pub fn new(prefix: String) -> Self {
        Self { prefix }
    }

    // Name format: It must have the format "projects/{project}/topics/{topic}". {topic} must start with a letter, and contain only letters (\[A-Za-z\]), numbers (\[0-9\]), dashes (-), underscores (_), periods (.), tildes (~), plus (+) or percent signs (%). It must be between 3 and 255 characters in length, and it must not start with "goog".
    fn transform_full_name(&self, name: String) -> String {
        let tokens = name.split('/').collect::<Vec<&str>>();
        if tokens.len() != 4 {
            panic!("Name must have the format 'projects/$project/$resource_type/$name'");
        }

        let project = tokens[1];
        let resource_type = tokens[2];
        let resource_name = tokens[3];

        let prefix = format!("{}--", self.prefix);

        let resource_name = if resource_name.starts_with(&prefix) {
            resource_name.to_string()
        } else {
            format!("{}{}", prefix, resource_name)
        };

        format!("projects/{}/{}/{}", project, resource_type, resource_name)
    }
}

impl ProxyInterceptor for NamespaceInterceptor {
    fn transform_create_topic(&self, request: super::Topic) -> super::Topic {
        super::Topic {
            name: self.transform_full_name(request.name),
            ..request
        }
    }

    fn transform_update_topic(
        &self,
        request: super::UpdateTopicRequest,
    ) -> super::UpdateTopicRequest {
        super::UpdateTopicRequest {
            topic: request.topic.map(|t| super::Topic {
                name: self.transform_full_name(t.name),
                ..t
            }),
            ..request
        }
    }

    fn transform_publish(&self, request: super::PublishRequest) -> super::PublishRequest {
        super::PublishRequest {
            topic: self.transform_full_name(request.topic),
            ..request
        }
    }

    fn transform_get_topic(&self, request: super::GetTopicRequest) -> super::GetTopicRequest {
        super::GetTopicRequest {
            topic: self.transform_full_name(request.topic),
            ..request
        }
    }

    fn transform_list_topic_subscriptions(
        &self,
        request: super::ListTopicSubscriptionsRequest,
    ) -> super::ListTopicSubscriptionsRequest {
        super::ListTopicSubscriptionsRequest {
            topic: self.transform_full_name(request.topic),
            ..request
        }
    }

    fn transform_list_topic_snapshots(
        &self,
        request: super::ListTopicSnapshotsRequest,
    ) -> super::ListTopicSnapshotsRequest {
        super::ListTopicSnapshotsRequest {
            topic: self.transform_full_name(request.topic),
            ..request
        }
    }

    fn transform_delete_topic(
        &self,
        request: super::DeleteTopicRequest,
    ) -> super::DeleteTopicRequest {
        super::DeleteTopicRequest {
            topic: self.transform_full_name(request.topic),
            ..request
        }
    }

    fn transform_get_subscription(
        &self,
        request: super::GetSubscriptionRequest,
    ) -> super::GetSubscriptionRequest {
        super::GetSubscriptionRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_delete_subscription(
        &self,
        request: super::DeleteSubscriptionRequest,
    ) -> super::DeleteSubscriptionRequest {
        super::DeleteSubscriptionRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_modify_ack_deadline(
        &self,
        request: super::ModifyAckDeadlineRequest,
    ) -> super::ModifyAckDeadlineRequest {
        super::ModifyAckDeadlineRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_pull(&self, request: super::PullRequest) -> super::PullRequest {
        super::PullRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_acknowledge(
        &self,
        request: super::AcknowledgeRequest,
    ) -> super::AcknowledgeRequest {
        super::AcknowledgeRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_modify_push_config(
        &self,
        request: super::ModifyPushConfigRequest,
    ) -> super::ModifyPushConfigRequest {
        super::ModifyPushConfigRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_detach_subscription(
        &self,
        request: super::DetachSubscriptionRequest,
    ) -> super::DetachSubscriptionRequest {
        super::DetachSubscriptionRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_create_subscription(&self, request: super::Subscription) -> super::Subscription {
        super::Subscription {
            name: self.transform_full_name(request.name),
            topic: self.transform_full_name(request.topic),
            ..request
        }
    }

    fn transform_update_subscription(
        &self,
        request: super::UpdateSubscriptionRequest,
    ) -> super::UpdateSubscriptionRequest {
        super::UpdateSubscriptionRequest {
            subscription: request.subscription.map(|s| super::Subscription {
                name: self.transform_full_name(s.name),
                topic: self.transform_full_name(s.topic),
                ..s
            }),
            ..request
        }
    }

    fn transform_streaming_pull(
        &self,
        request: super::StreamingPullRequest,
    ) -> super::StreamingPullRequest {
        super::StreamingPullRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_get_snapshot(
        &self,
        request: super::GetSnapshotRequest,
    ) -> super::GetSnapshotRequest {
        super::GetSnapshotRequest {
            snapshot: self.transform_full_name(request.snapshot),
            ..request
        }
    }

    fn transform_create_snapshot(
        &self,
        request: super::CreateSnapshotRequest,
    ) -> super::CreateSnapshotRequest {
        super::CreateSnapshotRequest {
            name: self.transform_full_name(request.name),
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_delete_snapshot(
        &self,
        request: super::DeleteSnapshotRequest,
    ) -> super::DeleteSnapshotRequest {
        super::DeleteSnapshotRequest {
            snapshot: self.transform_full_name(request.snapshot),
            ..request
        }
    }

    fn transform_seek(&self, request: super::SeekRequest) -> super::SeekRequest {
        super::SeekRequest {
            subscription: self.transform_full_name(request.subscription),
            ..request
        }
    }

    fn transform_update_snapshot(
        &self,
        request: super::UpdateSnapshotRequest,
    ) -> super::UpdateSnapshotRequest {
        super::UpdateSnapshotRequest {
            snapshot: request.snapshot.map(|s| super::Snapshot {
                name: self.transform_full_name(s.name),
                ..s
            }),
            ..request
        }
    }
}
