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

use quickwit_config::{CLI_SOURCE_ID, SourceConfig};
use quickwit_proto::metastore::{AddSourceRequest, IndexMetadataRequest};

use super::*;
use crate::{AddSourceRequestExt, IndexMetadataResponseExt};

fn checkpoint(range: std::ops::Range<u64>) -> IndexCheckpointDelta {
    IndexCheckpointDelta {
        source_id: CLI_SOURCE_ID.to_string(),
        source_delta: SourceCheckpointDelta::from_range(range),
    }
}

#[tokio::test]
async fn failed_replacement_rolls_back_splits_and_checkpoint() {
    for index_type in [IndexType::Metrics, IndexType::Sketches] {
        let metastore = metastore_for_test();
        let mut config = IndexConfig::for_test("cpu", "ram://cpu");
        config.index_type = index_type;
        let metadata = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&config).unwrap())
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    metadata.index_uid.clone(),
                    &SourceConfig::cli(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let catalog = ParquetSplits::from_index_metadata(metastore.clone(), &metadata).unwrap();
        catalog
            .stage(&[
                split(&catalog, "input"),
                split(&catalog, "output"),
                split(&catalog, "unpublished"),
            ])
            .await
            .unwrap();
        catalog
            .publish(&ParquetPublication {
                staged_split_ids: vec!["input".into()],
                checkpoint_delta: Some(checkpoint(0..10)),
                ..Default::default()
            })
            .await
            .unwrap();
        let before = metastore
            .index_metadata(IndexMetadataRequest::for_index_uid(
                metadata.index_uid.clone(),
            ))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        for invalid_input in ["missing", "unpublished"] {
            let error = catalog
                .publish(&ParquetPublication {
                    staged_split_ids: vec!["output".into()],
                    replaced_split_ids: vec![invalid_input.into()],
                    checkpoint_delta: Some(checkpoint(10..20)),
                    ..Default::default()
                })
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::FailedPrecondition { .. }));
            let after = metastore
                .index_metadata(IndexMetadataRequest::for_index_uid(
                    metadata.index_uid.clone(),
                ))
                .await
                .unwrap()
                .deserialize_index_metadata()
                .unwrap();
            assert_eq!(before.checkpoint, after.checkpoint);
            let states: Vec<_> = catalog
                .list_all(catalog.query().with_split_states([]))
                .await
                .unwrap()
                .into_iter()
                .map(|record| (record.metadata.split_id.to_string(), record.state))
                .collect();
            assert_eq!(
                states,
                [
                    ("input".into(), SplitState::Published),
                    ("output".into(), SplitState::Staged),
                    ("unpublished".into(), SplitState::Staged)
                ]
            );
        }
        catalog
            .publish(&ParquetPublication {
                staged_split_ids: vec!["output".into()],
                replaced_split_ids: vec!["input".into()],
                checkpoint_delta: Some(checkpoint(10..20)),
                ..Default::default()
            })
            .await
            .unwrap();
        // A stale merge cannot reuse inputs already replaced by a successful merge.
        let error = catalog
            .publish(&ParquetPublication {
                staged_split_ids: vec!["unpublished".into()],
                replaced_split_ids: vec!["input".into()],
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(error, MetastoreError::FailedPrecondition { .. }));
        let published = catalog.list_all(catalog.query()).await.unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].metadata.split_id.as_str(), "output");
    }
}
