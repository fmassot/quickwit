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

use super::*;

#[tokio::test]
async fn test_uploader_without_sequencer() -> anyhow::Result<()> {
    let node_id = NodeId::from_str("test-node");
    let index_uid = IndexUid::for_test("test-index", 0);
    let index_uid_clone = index_uid.clone();
    let source_id = "test-source".to_string();

    let universe = Universe::new();
    let (publisher_mailbox, publisher_inbox) = universe.create_test_mailbox::<Publisher>();
    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_stage_splits()
        .withf(move |stage_splits_request| -> bool {
            stage_splits_request.index_uid() == &index_uid_clone
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
        SplitsUpdateMailbox::Publisher(publisher_mailbox),
        4,
        EventBroker::default(),
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
                    split_id: "test-split".into(),
                    partition_id: 3u64,
                    time_range: None,
                    uncompressed_docs_size_in_bytes: 1_000,
                    num_docs: 10,
                    replaced_split_ids: Vec::new(),
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
    let SplitsUpdate {
        index_uid,
        new_splits,
        replaced_split_ids,
        ..
    } = publisher_inbox.recv_typed_message().await.unwrap();

    assert_eq!(index_uid.index_id, "test-index");
    assert_eq!(new_splits.len(), 1);
    assert!(replaced_split_ids.is_empty());
    universe.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_uploader_with_empty_splits() -> anyhow::Result<()> {
    let universe = Universe::new();
    let (sequencer_mailbox, sequencer_inbox) =
        universe.create_test_mailbox::<Sequencer<Publisher>>();
    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore.expect_stage_splits().never();
    let ram_storage = RamStorage::default();
    let split_store =
        IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
    let uploader = Uploader::new(
        UploaderType::IndexUploader,
        MetastoreServiceClient::from_mock(mock_metastore),
        default_merge_policy(),
        None,
        split_store,
        SplitsUpdateMailbox::Sequencer(sequencer_mailbox),
        4,
        EventBroker::default(),
    );
    let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
    let checkpoint_delta = IndexCheckpointDelta {
        source_id: "test-source".to_string(),
        source_delta: SourceCheckpointDelta::from_range(3..15),
    };
    uploader_mailbox
        .send_message(EmptySplit {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            checkpoint_delta,
            publish_lock: PublishLock::default(),
            batch_parent_span: Span::none(),
        })
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
            "Expected `SequencerCommand::Proceed(SplitUpdate)`, got `SequencerCommand::Discard`."
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
    assert_eq!(new_splits.len(), 0);
    let checkpoint_delta = checkpoint_delta_opt.unwrap();
    assert_eq!(checkpoint_delta.source_id, "test-source");
    assert_eq!(
        checkpoint_delta.source_delta,
        SourceCheckpointDelta::from_range(3..15)
    );
    assert!(replaced_split_ids.is_empty());
    let files = ram_storage.list_files().await;
    assert!(files.is_empty());
    universe.assert_quit().await;
    Ok(())
}

struct ReportSplitListener {
    report_splits_tx: flume::Sender<ReportSplitsRequest>,
}

impl std::fmt::Debug for ReportSplitListener {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ReportSplitListener").finish()
    }
}

#[async_trait]
impl EventSubscriber<ReportSplitsRequest> for ReportSplitListener {
    async fn handle_event(&mut self, event: ReportSplitsRequest) {
        self.report_splits_tx.send(event).unwrap();
    }
}

#[tokio::test]
async fn test_uploader_notifies_event_broker() -> anyhow::Result<()> {
    quickwit_common::setup_logging_for_tests();
    const SPLIT_ULID_STR: &str = "01HAV29D4XY3D462FS3D8K5Q2H";
    let event_broker = EventBroker::default();
    let (report_splits_tx, report_splits_rx) = flume::unbounded();
    let report_splits_listener = ReportSplitListener { report_splits_tx };

    // we need to keep the handle alive.
    let _subscribe_handle = event_broker.subscribe(report_splits_listener);

    let node_id = NodeId::from_str("test-node");
    let index_uid = IndexUid::new_with_random_ulid("test-index");
    let source_id = "test-source".to_string();

    let universe = Universe::new();
    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_stage_splits()
        .times(1)
        .returning(|_| Ok(EmptyResponse {}));
    let ram_storage = RamStorage::default();
    let split_store =
        IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
    let merge_policy = Arc::new(NopMergePolicy);
    let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox();
    let uploader = Uploader::new(
        UploaderType::IndexUploader,
        MetastoreServiceClient::from_mock(mock_metastore),
        merge_policy,
        None,
        split_store,
        SplitsUpdateMailbox::Publisher(publisher_mailbox),
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
                    split_id: SPLIT_ULID_STR.into(),
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
    mem::drop(uploader_mailbox);
    let report_splits: ReportSplitsRequest = report_splits_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(report_splits.report_splits.len(), 1);
    let split = &report_splits.report_splits[0];
    assert_eq!(split.storage_uri, "ram:///");
    assert_eq!(split.split_id, SPLIT_ULID_STR);
    universe.assert_quit().await;
    Ok(())
}
