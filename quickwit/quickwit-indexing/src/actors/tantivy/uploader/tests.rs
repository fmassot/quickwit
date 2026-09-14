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

mod delivery;
use std::mem;
use std::path::PathBuf;
use std::time::Duration;

use ::tantivy::DateTime;
use quickwit_actors::{ObservationType, Universe};
use quickwit_common::pubsub::EventSubscriber;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
use quickwit_proto::types::{DocMappingUid, IndexUid, NodeId, SplitId};
use quickwit_storage::RamStorage;
use tokio::sync::oneshot;
use tracing::Span;

use super::*;
use crate::actors::sequencer::SequencerCommand;
use crate::actors::{Publisher, Sequencer, Uploader};
use crate::merge_policy::{NopMergePolicy, default_merge_policy};
use crate::models::{SplitAttrs, SplitsUpdate};

#[test]
fn test_split_recovery_metadata_preserves_maturity() {
    let split_metadata = SplitMetadata {
        maturity: SplitMaturity::Immature {
            maturation_period: Duration::from_millis(1_500),
        },
        ..Default::default()
    };

    let recovery_metadata = create_split_recovery_metadata(&split_metadata, &[]);
    assert_eq!(recovery_metadata.maturation_period_millis, Some(1_500));

    let (recovered_metadata, _parent_split_ids) =
        SplitMetadata::try_from_recovery_metadata(recovery_metadata, 1..2).unwrap();
    assert_eq!(recovered_metadata.maturity, split_metadata.maturity);

    let mature_split_metadata = SplitMetadata::default();
    let mature_recovery_metadata = create_split_recovery_metadata(&mature_split_metadata, &[]);
    assert_eq!(mature_recovery_metadata.maturation_period_millis, None);
    let (recovered_mature_metadata, _parent_split_ids) =
        SplitMetadata::try_from_recovery_metadata(mature_recovery_metadata, 1..2).unwrap();
    assert_eq!(recovered_mature_metadata.maturity, SplitMaturity::Mature);
}

#[tokio::test]
async fn test_uploader_with_sequencer() -> anyhow::Result<()> {
    quickwit_common::setup_logging_for_tests();

    let node_id = NodeId::from_str("test-node");
    let index_uid = IndexUid::new_with_random_ulid("test-index");
    let source_id = "test-source".to_string();

    let event_broker = EventBroker::default();
    let universe = Universe::new();
    let (sequencer_mailbox, sequencer_inbox) =
        universe.create_test_mailbox::<Sequencer<Publisher>>();
    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_stage_splits()
        .withf(move |stage_splits_request| -> bool {
            let splits_metadata = stage_splits_request.deserialize_splits_metadata().unwrap();
            let split_metadata = &splits_metadata[0];
            let index_uid: IndexUid = stage_splits_request.index_uid().clone();
            index_uid.index_id == "test-index"
                && split_metadata.split_id() == "test-split"
                && split_metadata.time_range == Some(1628203589..=1628203640)
        })
        .times(1)
        .returning(|_| Ok(EmptyResponse {}));
    let ram_storage = RamStorage::default();
    let split_store =
        IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
    let merge_policy = Arc::new(NopMergePolicy);
    let uploader = Uploader::new(
        UploaderType::IndexUploader,
        MetastoreServiceClient::from_mock(mock_metastore),
        merge_policy,
        None,
        split_store,
        SplitsUpdateMailbox::Sequencer(sequencer_mailbox),
        4,
        event_broker,
    );
    let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
    let split_scratch_directory = TempDirectory::for_test();
    let checkpoint_delta_opt: Option<IndexCheckpointDelta> = Some(IndexCheckpointDelta {
        source_id: "test-source".to_string(),
        source_delta: SourceCheckpointDelta::from_range(3..15),
    });
    uploader_mailbox
        .send_message(PackagedSplitBatch::new(
            vec![PackagedSplit {
                split_attrs: SplitAttrs {
                    node_id,
                    index_uid,
                    source_id,
                    doc_mapping_uid: DocMappingUid::default(),
                    partition_id: 3u64,
                    time_range: Some(
                        DateTime::from_timestamp_secs(1_628_203_589)
                            ..=DateTime::from_timestamp_secs(1_628_203_640),
                    ),
                    uncompressed_docs_size_in_bytes: 1_000,
                    num_docs: 10,
                    replaced_split_ids: Vec::new(),
                    split_id: "test-split".into(),
                    delete_opstamp: 10,
                    num_merge_ops: 0,
                },
                serialized_split_fields: Vec::new(),
                split_scratch_directory,
                tags: Default::default(),
                hotcache_bytes: Vec::new(),
                split_files: Vec::new(),
            }],
            checkpoint_delta_opt,
            PublishLock::default(),
            None,
            Span::none(),
        ))
        .await?;
    assert_eq!(
        uploader_handle.process_pending_and_observe().await.obs_type,
        ObservationType::Alive
    );
    let mut publish_futures: Vec<oneshot::Receiver<SequencerCommand<SplitsUpdate>>> =
        sequencer_inbox.drain_for_test_typed();
    assert_eq!(publish_futures.len(), 1);

    let publisher_message = match publish_futures.pop().unwrap().await? {
        SequencerCommand::Discard => panic!(
            "expected `SequencerCommand::Proceed(SplitUpdate)`, got `SequencerCommand::Discard`"
        ),
        SequencerCommand::Proceed(publisher_message) => publisher_message,
    };
    let SplitsUpdate {
        index_uid,
        new_splits,
        checkpoint_delta_opt,
        replaced_split_ids,
        ..
    } = publisher_message;

    assert_eq!(index_uid.index_id, "test-index");
    assert_eq!(new_splits.len(), 1);
    assert_eq!(new_splits[0].split_id(), "test-split");
    let checkpoint_delta = checkpoint_delta_opt.unwrap();
    assert_eq!(checkpoint_delta.source_id, "test-source");
    assert_eq!(
        checkpoint_delta.source_delta,
        SourceCheckpointDelta::from_range(3..15)
    );
    assert!(replaced_split_ids.is_empty());
    let mut files = ram_storage.list_files().await;
    files.sort();
    assert_eq!(&files, &[PathBuf::from("test-split.split")]);
    universe.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_uploader_with_sequencer_emits_replace() -> anyhow::Result<()> {
    let node_id = NodeId::from_str("test-node");
    let index_uid = IndexUid::new_with_random_ulid("test-index");
    let source_id = "test-source".to_string();

    let universe = Universe::new();
    let (sequencer_mailbox, sequencer_inbox) =
        universe.create_test_mailbox::<Sequencer<Publisher>>();
    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_stage_splits()
        .withf(move |stage_splits_request| -> bool {
            let splits_metadata = stage_splits_request.deserialize_splits_metadata().unwrap();
            let is_metadata_valid = splits_metadata.iter().all(|metadata| {
                ["test-split-1", "test-split-2"].contains(&metadata.split_id().as_str())
                    && metadata.time_range == Some(1628203589..=1628203640)
            });
            let index_uid: IndexUid = stage_splits_request.index_uid().clone();
            index_uid.index_id == "test-index" && is_metadata_valid
        })
        .times(1)
        .returning(|_| Ok(EmptyResponse {}));
    let ram_storage = RamStorage::default();
    let split_store =
        IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
    let merge_policy = Arc::new(NopMergePolicy);
    let uploader = Uploader::new(
        UploaderType::IndexUploader,
        MetastoreServiceClient::from_mock(mock_metastore),
        merge_policy,
        None,
        split_store,
        SplitsUpdateMailbox::Sequencer(sequencer_mailbox),
        4,
        EventBroker::default(),
    );
    let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
    let split_scratch_directory_1 = TempDirectory::for_test();
    let split_scratch_directory_2 = TempDirectory::for_test();
    let packaged_split_1 = PackagedSplit {
        split_attrs: SplitAttrs {
            node_id: node_id.clone(),
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            doc_mapping_uid: DocMappingUid::default(),
            split_id: "test-split-1".into(),
            partition_id: 3u64,
            num_docs: 10,
            uncompressed_docs_size_in_bytes: 1_000,
            time_range: Some(
                DateTime::from_timestamp_secs(1_628_203_589)
                    ..=DateTime::from_timestamp_secs(1_628_203_640),
            ),
            replaced_split_ids: vec![
                SplitId::from("replaced-split-1"),
                SplitId::from("replaced-split-2"),
            ],
            delete_opstamp: 0,
            num_merge_ops: 0,
        },
        serialized_split_fields: Vec::new(),
        split_scratch_directory: split_scratch_directory_1,
        tags: Default::default(),
        split_files: Vec::new(),
        hotcache_bytes: Vec::new(),
    };
    let package_split_2 = PackagedSplit {
        split_attrs: SplitAttrs {
            node_id,
            index_uid,
            source_id,
            doc_mapping_uid: DocMappingUid::default(),
            split_id: "test-split-2".into(),
            partition_id: 3u64,
            num_docs: 10,
            uncompressed_docs_size_in_bytes: 1_000,
            time_range: Some(
                DateTime::from_timestamp_secs(1_628_203_589)
                    ..=DateTime::from_timestamp_secs(1_628_203_640),
            ),
            replaced_split_ids: vec![
                SplitId::from("replaced-split-1"),
                SplitId::from("replaced-split-2"),
            ],
            delete_opstamp: 0,
            num_merge_ops: 0,
        },
        serialized_split_fields: Vec::new(),
        split_scratch_directory: split_scratch_directory_2,
        tags: Default::default(),
        split_files: Vec::new(),
        hotcache_bytes: Vec::new(),
    };
    uploader_mailbox
        .send_message(PackagedSplitBatch::new(
            vec![packaged_split_1, package_split_2],
            None,
            PublishLock::default(),
            None,
            Span::none(),
        ))
        .await?;
    assert_eq!(
        uploader_handle.process_pending_and_observe().await.obs_type,
        ObservationType::Alive
    );
    let mut publish_futures: Vec<oneshot::Receiver<SequencerCommand<SplitsUpdate>>> =
        sequencer_inbox.drain_for_test_typed();
    assert_eq!(publish_futures.len(), 1);

    let publisher_message = match publish_futures.pop().unwrap().await? {
        SequencerCommand::Discard => panic!(
            "Expected `SequencerCommand::Proceed(SplitsUpdate)`, got `SequencerCommand::Discard`."
        ),
        SequencerCommand::Proceed(publisher_message) => publisher_message,
    };
    let SplitsUpdate {
        index_uid,
        new_splits,
        mut replaced_split_ids,
        checkpoint_delta_opt,
        ..
    } = publisher_message;
    assert_eq!(index_uid.index_id, "test-index");
    // Sort first to avoid test failing.
    replaced_split_ids.sort();
    assert_eq!(new_splits.len(), 2);
    assert_eq!(new_splits[0].split_id(), "test-split-1");
    assert_eq!(new_splits[1].split_id(), "test-split-2");
    assert_eq!(
        &replaced_split_ids,
        &[
            SplitId::from("replaced-split-1"),
            SplitId::from("replaced-split-2"),
        ]
    );
    assert!(checkpoint_delta_opt.is_none());

    let mut files = ram_storage.list_files().await;
    files.sort();
    assert_eq!(
        &files,
        &[
            PathBuf::from("test-split-1.split"),
            PathBuf::from("test-split-2.split")
        ]
    );
    universe.assert_quit().await;
    Ok(())
}
