// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use quickwit_actors::{Command, QueueCapacity, Universe};
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_parquet_engine::split::{
    ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata, TimeRange,
};
use quickwit_proto::metastore::{
    EmptyResponse, MetastoreError, MetastoreServiceClient, MockMetastoreService,
};
use quickwit_proto::types::IndexUid;
use tracing::Span;

use crate::actors::parquet_pipeline::{ParquetPublisher as Publisher, ParquetSplitsUpdate};
use crate::models::{PublishLock, SharedPublishToken};

fn create_test_metrics_split_metadata(index_uid: &str, split_id: &str) -> ParquetSplitMetadata {
    ParquetSplitMetadata::metrics_builder()
        .index_uid(index_uid)
        .split_id(ParquetSplitId::new(split_id))
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(1024)
        .build()
}

#[tokio::test]
async fn test_metrics_publisher_publishes_splits() {
    let universe = Universe::with_accelerated_time();

    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_publish_metrics_splits()
        .withf(|request| {
            request.index_uid().to_string().starts_with("test-index:")
                && request.staged_split_ids == vec!["split-1".to_string()]
                && request.replaced_split_ids.is_empty()
                && request.index_checkpoint_delta_json_opt.is_some()
                && request.publish_token_opt.is_none()
        })
        .times(1)
        .returning(|_| Ok(EmptyResponse {}));

    let publisher = Publisher::new_parquet(
        ParquetSplitKind::Metrics,
        QueueCapacity::Bounded(1),
        MetastoreServiceClient::from_mock(mock_metastore),
        None,
        SharedPublishToken::default(),
    );
    let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

    let update = ParquetSplitsUpdate {
        index_uid: IndexUid::for_test("test-index", 0),
        new_splits: vec![create_test_metrics_split_metadata(
            "test-index:00000000000000000000000000",
            "split-1",
        )],
        replaced_split_ids: Vec::new(),
        checkpoint_delta_opt: Some(IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(0..10),
        }),
        publish_lock: PublishLock::default(),
        parent_span: Span::none(),
        merge_task: None,
    };

    publisher_mailbox.send_message(update).await.unwrap();

    let observation = publisher_handle.process_pending_and_observe().await.state;
    assert_eq!(observation.num_published_splits, 1);
    assert_eq!(observation.num_replace_operations, 0);
    assert_eq!(observation.num_empty_splits, 0);

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_metrics_publisher_handles_empty_splits() {
    let universe = Universe::with_accelerated_time();

    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_publish_metrics_splits()
        .withf(|request| {
            request.index_uid().to_string().starts_with("test-index:")
                && request.staged_split_ids.is_empty()
                && request.replaced_split_ids.is_empty()
                && request.index_checkpoint_delta_json_opt.is_some()
        })
        .times(1)
        .returning(|_| Ok(EmptyResponse {}));

    let publisher = Publisher::new_parquet(
        ParquetSplitKind::Metrics,
        QueueCapacity::Bounded(1),
        MetastoreServiceClient::from_mock(mock_metastore),
        None,
        SharedPublishToken::default(),
    );
    let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

    let update = ParquetSplitsUpdate {
        index_uid: IndexUid::for_test("test-index", 0),
        new_splits: Vec::new(),
        replaced_split_ids: Vec::new(),
        checkpoint_delta_opt: Some(IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(0..1),
        }),
        publish_lock: PublishLock::default(),
        parent_span: Span::none(),
        merge_task: None,
    };

    publisher_mailbox.send_message(update).await.unwrap();

    let observation = publisher_handle.process_pending_and_observe().await.state;
    assert_eq!(observation.num_published_splits, 0);
    assert_eq!(observation.num_replace_operations, 0);
    assert_eq!(observation.num_empty_splits, 1);

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_sketch_publisher_handles_empty_splits_without_name_prefix() {
    let universe = Universe::with_accelerated_time();
    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore.expect_publish_metrics_splits().never();
    mock_metastore
        .expect_publish_sketch_splits()
        .withf(|request| {
            request.index_uid().index_id == "cpu"
                && request.staged_split_ids.is_empty()
                && request.replaced_split_ids.is_empty()
                && request.index_checkpoint_delta_json_opt.is_some()
        })
        .times(1)
        .returning(|_| Ok(EmptyResponse {}));
    let publisher = Publisher::new_parquet(
        ParquetSplitKind::Sketches,
        QueueCapacity::Bounded(1),
        MetastoreServiceClient::from_mock(mock_metastore),
        None,
        SharedPublishToken::default(),
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(publisher);
    mailbox
        .send_message(ParquetSplitsUpdate {
            index_uid: IndexUid::for_test("cpu", 0),
            new_splits: Vec::new(),
            replaced_split_ids: Vec::new(),
            checkpoint_delta_opt: Some(IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range(0..1),
            }),
            publish_lock: PublishLock::default(),
            parent_span: Span::none(),
            merge_task: None,
        })
        .await
        .unwrap();
    assert_eq!(
        handle
            .process_pending_and_observe()
            .await
            .state
            .num_empty_splits,
        1
    );
    universe.assert_quit().await;
}

#[tokio::test]
async fn test_metrics_publisher_respects_publish_lock() {
    let universe = Universe::with_accelerated_time();

    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore.expect_publish_metrics_splits().never();

    let publisher = Publisher::new_parquet(
        ParquetSplitKind::Metrics,
        QueueCapacity::Bounded(1),
        MetastoreServiceClient::from_mock(mock_metastore),
        None,
        SharedPublishToken::default(),
    );
    let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

    let publish_lock = PublishLock::default();
    publish_lock.kill().await;

    let update = ParquetSplitsUpdate {
        index_uid: IndexUid::for_test("test-index", 0),
        new_splits: vec![create_test_metrics_split_metadata(
            "test-index:00000000000000000000000000",
            "split-1",
        )],
        replaced_split_ids: Vec::new(),
        checkpoint_delta_opt: Some(IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(0..10),
        }),
        publish_lock,
        parent_span: Span::none(),
        merge_task: None,
    };

    publisher_mailbox.send_message(update).await.unwrap();

    let observation = publisher_handle.process_pending_and_observe().await.state;
    assert_eq!(observation.num_published_splits, 0);
    assert_eq!(observation.num_replace_operations, 0);
    assert_eq!(observation.num_empty_splits, 0);

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_metrics_publisher_terminates_pipeline_on_invalid_publish_token_error() {
    let universe = Universe::with_accelerated_time();

    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_publish_metrics_splits()
        .times(3)
        .returning(|_| {
            Err(MetastoreError::InvalidPublishToken {
                queue_id: "test-index:0/test-source/0".to_string(),
            })
        });
    let (source_mailbox, source_inbox) = universe.create_test_mailbox();
    let publisher = Publisher::new_parquet(
        ParquetSplitKind::Metrics,
        QueueCapacity::Bounded(1),
        MetastoreServiceClient::from_mock(mock_metastore),
        Some(source_mailbox),
        SharedPublishToken::default(),
    );
    let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
    let publish_lock = PublishLock::default();
    let splits_update = |split_id: &str| ParquetSplitsUpdate {
        index_uid: IndexUid::for_test("test-index", 0),
        new_splits: vec![create_test_metrics_split_metadata(
            "test-index:00000000000000000000000000",
            split_id,
        )],
        replaced_split_ids: Vec::new(),
        checkpoint_delta_opt: Some(IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(0..10),
        }),
        publish_lock: publish_lock.clone(),
        parent_span: Span::none(),
        merge_task: None,
    };
    publisher_mailbox
        .send_message(splits_update("split-1"))
        .await
        .unwrap();
    wait_until_predicate(
        || {
            let publish_lock = publish_lock.clone();
            async move { publish_lock.is_dead() }
        },
        Duration::from_secs(10),
        Duration::from_millis(10),
    )
    .await
    .expect("publisher should give up on the revoked token and kill the publish lock");

    publisher_mailbox
        .send_message(splits_update("split-2"))
        .await
        .unwrap();
    drop(publisher_mailbox);
    let (exit_status, observation) = publisher_handle.join().await;

    assert!(exit_status.is_success());
    assert_eq!(observation.num_published_splits, 0);
    let source_commands = source_inbox.drain_for_test_typed::<Command>();
    assert!(matches!(
        source_commands.as_slice(),
        [Command::ExitWithSuccess]
    ));
    universe.assert_quit().await;
}
