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

#![allow(clippy::disallowed_methods)]
use quickwit_actors::{ObservationType, Universe};
use quickwit_metastore::StageParquetSplitsRequestExt;
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_parquet_engine::split::{ParquetSplitMetadata, TimeRange};
use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
use quickwit_proto::types::IndexUid;
use quickwit_storage::RamStorage;

use super::*;
use crate::actors::{ParquetPublisher as Publisher, Sequencer};
use crate::models::PublishLock;

fn create_test_metrics_split(index_id: &str, split_id: &str) -> ParquetSplitMetadata {
    ParquetSplitMetadata::metrics_builder()
        .index_uid(IndexUid::for_test(index_id, 0).to_string())
        .split_id(quickwit_parquet_engine::split::ParquetSplitId::new(
            split_id,
        ))
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(1024)
        .build()
}

/// Create placeholder parquet files in the temp directory for testing.
/// The uploader expects to read these files from output_dir.
fn create_placeholder_parquet_files(temp_dir: &std::path::Path, splits: &[ParquetSplitMetadata]) {
    for split in splits {
        let parquet_filename = split.parquet_filename();
        let file_path = temp_dir.join(&parquet_filename);
        // Write minimal valid content (actual parquet not needed for staging test)
        std::fs::write(&file_path, b"placeholder parquet content")
            .expect("Failed to create placeholder parquet file");
    }
}

#[tokio::test]
async fn test_metrics_uploader_stages_and_uploads() {
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::new();
    let temp_dir = tempfile::tempdir().unwrap();
    let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
    let sequencer_mailbox = super::super::spawn_sequencer_for_test(&universe, publisher_mailbox);

    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
            .expect_stage_metrics_splits()
            .withf(|request| {
                if request.index_uid().index_id != "test-index" {
                    return false;
                }
                let splits = request.deserialize_splits_metadata().unwrap();
                matches!(
                    splits.as_slice(),
                    [split]
                        if matches!(
                            split.maturity,
                            quickwit_parquet_engine::merge::policy::ParquetSplitMaturity::Immature { .. }
                        )
                )
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

    let ram_storage = Arc::new(RamStorage::default());
    let uploader = ParquetUploader::new(
        UploaderType::IndexUploader,
        MetastoreServiceClient::from_mock(mock_metastore),
        ram_storage.clone(),
        sequencer_mailbox,
        4,
        crate::merge_policy::parquet_merge_policy_from_settings(
            &quickwit_config::IndexingSettings::default(),
        ),
    );

    let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

    // Create test batch with temp directory as output_dir
    let splits = vec![create_test_metrics_split("test-index", "test-split-1")];
    // Create placeholder parquet files that the uploader will read
    create_placeholder_parquet_files(temp_dir.path(), &splits);
    let checkpoint_delta = IndexCheckpointDelta {
        source_id: "test-source".to_string(),
        source_delta: SourceCheckpointDelta::from_range(0..10),
    };
    let batch = ParquetSplitBatch {
        index_uid: IndexUid::for_test("test-index", 0),
        splits,
        output_dir: temp_dir.path().to_path_buf(),
        checkpoint_delta_opt: Some(checkpoint_delta),
        publish_lock: PublishLock::default(),
        replaced_split_ids: Vec::new(),
        _scratch_directory_opt: None,
        _merge_task_opt: None,
    };

    uploader_mailbox.send_message(batch).await.unwrap();

    let observation = uploader_handle.process_pending_and_observe().await;
    assert_eq!(observation.obs_type, ObservationType::Alive);

    // Verify counters
    assert!(observation.state.num_staged_splits.load(Ordering::Relaxed) >= 1);

    // Wait briefly for the async upload task to complete file deletion
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify local parquet files are deleted after successful upload
    let remaining_parquet_files: Vec<_> = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
        })
        .collect();
    assert!(
        remaining_parquet_files.is_empty(),
        "expected parquet files to be deleted after upload, but found: {:?}",
        remaining_parquet_files
            .iter()
            .map(|e| e.path())
            .collect::<Vec<_>>()
    );

    // Verify the file was actually uploaded to storage
    assert!(
        ram_storage
            .exists(std::path::Path::new("test-split-1.parquet"))
            .await
            .unwrap()
    );

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_metrics_uploader_deletes_local_files_after_upload() {
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::new();
    let temp_dir = tempfile::tempdir().unwrap();
    let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
    let sequencer_mailbox = super::super::spawn_sequencer_for_test(&universe, publisher_mailbox);

    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_stage_metrics_splits()
        .times(1)
        .returning(|_| Ok(EmptyResponse {}));

    let ram_storage = Arc::new(RamStorage::default());
    let uploader = ParquetUploader::new(
        UploaderType::IndexUploader,
        MetastoreServiceClient::from_mock(mock_metastore),
        ram_storage.clone(),
        sequencer_mailbox,
        4,
        crate::merge_policy::parquet_merge_policy_from_settings(
            &quickwit_config::IndexingSettings::default(),
        ),
    );

    let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

    // Create multiple splits with parquet files
    let splits = vec![
        create_test_metrics_split("test-index", "split-a"),
        create_test_metrics_split("test-index", "split-b"),
    ];
    create_placeholder_parquet_files(temp_dir.path(), &splits);

    // Verify files exist before upload
    assert!(temp_dir.path().join("split-a.parquet").exists());
    assert!(temp_dir.path().join("split-b.parquet").exists());

    let checkpoint_delta = IndexCheckpointDelta {
        source_id: "test-source".to_string(),
        source_delta: SourceCheckpointDelta::from_range(0..10),
    };
    let batch = ParquetSplitBatch {
        index_uid: IndexUid::for_test("test-index", 0),
        splits,
        output_dir: temp_dir.path().to_path_buf(),
        checkpoint_delta_opt: Some(checkpoint_delta),
        publish_lock: PublishLock::default(),
        replaced_split_ids: Vec::new(),
        _scratch_directory_opt: None,
        _merge_task_opt: None,
    };

    uploader_mailbox.send_message(batch).await.unwrap();

    let observation = uploader_handle.process_pending_and_observe().await;
    assert_eq!(observation.obs_type, ObservationType::Alive);

    // Wait for async upload task to complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Both local files should be deleted
    assert!(
        !temp_dir.path().join("split-a.parquet").exists(),
        "split-a.parquet should be deleted after upload"
    );
    assert!(
        !temp_dir.path().join("split-b.parquet").exists(),
        "split-b.parquet should be deleted after upload"
    );

    // Both files should exist in remote storage
    assert!(
        ram_storage
            .exists(std::path::Path::new("split-a.parquet"))
            .await
            .unwrap()
    );
    assert!(
        ram_storage
            .exists(std::path::Path::new("split-b.parquet"))
            .await
            .unwrap()
    );

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_metrics_uploader_handles_empty_batch() {
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::new();
    let temp_dir = tempfile::tempdir().unwrap();
    let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
    let sequencer_mailbox = super::super::spawn_sequencer_for_test(&universe, publisher_mailbox);

    let mut mock_metastore = MockMetastoreService::new();
    // Should NOT call stage_metrics_splits for empty batch
    mock_metastore.expect_stage_metrics_splits().never();

    let ram_storage = Arc::new(RamStorage::default());
    let uploader = ParquetUploader::new(
        UploaderType::IndexUploader,
        MetastoreServiceClient::from_mock(mock_metastore),
        ram_storage.clone(),
        sequencer_mailbox,
        4,
        crate::merge_policy::parquet_merge_policy_from_settings(
            &quickwit_config::IndexingSettings::default(),
        ),
    );

    let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

    // Create empty batch with valid checkpoint delta (1 position)
    let checkpoint_delta = IndexCheckpointDelta {
        source_id: "test-source".to_string(),
        source_delta: SourceCheckpointDelta::from_range(0..1),
    };
    let batch = ParquetSplitBatch {
        index_uid: IndexUid::new_with_random_ulid("test-index"),
        splits: Vec::new(),
        output_dir: temp_dir.path().to_path_buf(),
        checkpoint_delta_opt: Some(checkpoint_delta),
        publish_lock: PublishLock::default(),
        replaced_split_ids: Vec::new(),
        _scratch_directory_opt: None,
        _merge_task_opt: None,
    };

    uploader_mailbox.send_message(batch).await.unwrap();

    let observation = uploader_handle.process_pending_and_observe().await;
    assert_eq!(observation.obs_type, ObservationType::Alive);

    // Should not have staged any splits
    assert_eq!(
        observation.state.num_staged_splits.load(Ordering::Relaxed),
        0
    );

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_metrics_uploader_with_sequencer_ordering() {
    // This test verifies that when using the Sequencer variant:
    // 1. Messages flow through the sequencer to the publisher
    // 2. The sequencer maintains FIFO ordering even if uploads complete out of order
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::new();
    let temp_dir = tempfile::tempdir().unwrap();

    // Create a simple receiver actor to collect ParquetSplitsUpdate messages
    // We use a test mailbox for Publisher to capture what would be sent
    let (publisher_mailbox, publisher_inbox) = universe.create_test_mailbox::<Publisher>();

    // Create sequencer that forwards to publisher
    let sequencer = Sequencer::new(publisher_mailbox);
    let (sequencer_mailbox, _sequencer_handle) = universe.spawn_builder().spawn(sequencer);

    let mut mock_metastore = MockMetastoreService::new();
    // Allow multiple stage calls for the batches
    mock_metastore
        .expect_stage_metrics_splits()
        .returning(|_| Ok(EmptyResponse {}));

    let ram_storage = Arc::new(RamStorage::default());
    let uploader = ParquetUploader::new(
        UploaderType::IndexUploader,
        MetastoreServiceClient::from_mock(mock_metastore),
        ram_storage.clone(),
        sequencer_mailbox,
        4,
        crate::merge_policy::parquet_merge_policy_from_settings(
            &quickwit_config::IndexingSettings::default(),
        ),
    );

    let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

    // Send batches with splits that can be identified by split_id
    for i in 1..=3 {
        let splits = vec![create_test_metrics_split(
            "test-index",
            &format!("split-{}", i),
        )];
        // Create placeholder parquet files that the uploader will read
        create_placeholder_parquet_files(temp_dir.path(), &splits);
        let checkpoint_delta = IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range((i * 10)..(i * 10 + 10)),
        };
        let batch = ParquetSplitBatch {
            index_uid: IndexUid::for_test("test-index", 0),
            splits,
            output_dir: temp_dir.path().to_path_buf(),
            checkpoint_delta_opt: Some(checkpoint_delta),
            publish_lock: PublishLock::default(),
            replaced_split_ids: Vec::new(),
            _scratch_directory_opt: None,
            _merge_task_opt: None,
        };
        uploader_mailbox.send_message(batch).await.unwrap();
    }

    // Wait for all messages to be processed
    // The uploader spawns background tasks, so we need to give them time
    uploader_handle.process_pending_and_observe().await;

    // Give background tasks time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Drain inbox and verify ordering
    let mut received_split_ids = Vec::new();
    let messages = publisher_inbox.drain_for_test();
    for msg in messages {
        // The inbox contains typed messages, we need to access the ParquetSplitsUpdate
        if let Some(update) = msg.downcast_ref::<ParquetSplitsUpdate>() {
            for split in &update.new_splits {
                received_split_ids.push(split.split_id_str().to_string());
            }
        }
    }

    // Verify we received all 3 splits in order
    // The sequencer ensures FIFO delivery: split-1, split-2, split-3
    assert_eq!(
        received_split_ids,
        vec!["split-1", "split-2", "split-3"],
        "Sequencer should maintain FIFO ordering"
    );

    universe.assert_quit().await;
}
