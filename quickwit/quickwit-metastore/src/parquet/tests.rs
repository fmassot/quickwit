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

mod atomicity;

use quickwit_config::IndexConfig;
use quickwit_parquet_engine::split::{ParquetSplitId, TimeRange};
use quickwit_proto::metastore::{
    CreateIndexRequest, EmptyResponse, ListMetricsSplitsResponse, MetastoreService,
    MockMetastoreService,
};

use super::*;
use crate::checkpoint::SourceCheckpointDelta;
use crate::{
    CreateIndexRequestExt, CreateIndexResponseExt, ListParquetSplitsRequestExt,
    ListParquetSplitsResponseExt, PublishParquetSplitsRequestExt, SplitState, metastore_for_test,
};

fn split(catalog: &ParquetSplits, id: &str) -> ParquetSplitMetadata {
    let mut split = ParquetSplitMetadata::metrics_builder()
        .index_uid(catalog.index_uid().to_string())
        .split_id(ParquetSplitId::new(id))
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(17)
        .size_bytes(42)
        .build();
    split.kind = catalog.kind();
    split
}

fn record(catalog: &ParquetSplits, id: &str) -> ParquetSplitRecord {
    ParquetSplitRecord {
        state: SplitState::Published,
        update_timestamp: 0,
        metadata: split(catalog, id),
    }
}

fn mocked(mock: MockMetastoreService) -> ParquetSplits {
    ParquetSplits::new(
        MetastoreServiceClient::from_mock(mock),
        IndexUid::for_test("cpu", 0),
        ParquetSplitKind::Metrics,
    )
}

#[tokio::test]
async fn lifecycle_for_both_engines() {
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
        let catalog = ParquetSplits::from_index_metadata(metastore.clone(), &metadata).unwrap();
        let input = split(&catalog, "input");
        catalog.stage(std::slice::from_ref(&input)).await.unwrap();
        assert!(catalog.list_all(catalog.query()).await.unwrap().is_empty());
        // Deleting unmarked metadata must fail without changing its state.
        assert!(catalog.delete(&["input".to_string()]).await.is_err());
        catalog
            .publish(&ParquetPublication {
                staged_split_ids: vec!["input".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let published = catalog.list_all(catalog.query()).await.unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].metadata.num_rows, 17);
        assert_eq!(published[0].metadata.kind, catalog.kind());

        catalog.stage(&[split(&catalog, "merged")]).await.unwrap();
        catalog
            .publish(&ParquetPublication {
                staged_split_ids: vec!["merged".to_string()],
                replaced_split_ids: vec!["input".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let published = catalog.list_all(catalog.query()).await.unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].metadata.split_id.as_str(), "merged");
        let marked = catalog
            .list_all(
                catalog
                    .query()
                    .with_split_states([SplitState::MarkedForDeletion]),
            )
            .await
            .unwrap();
        assert_eq!(marked.len(), 1);
        assert_eq!(marked[0].metadata.split_id.as_str(), "input");
        catalog.delete(&["input".to_string()]).await.unwrap();
        catalog
            .mark_for_deletion(&["merged".to_string()])
            .await
            .unwrap();
        catalog.delete(&["merged".to_string()]).await.unwrap();
        assert!(
            catalog
                .list_all(catalog.query().with_split_states([]))
                .await
                .unwrap()
                .is_empty()
        );
        // Discovery rejects even a metrics-looking Tantivy name.
        let mut tantivy = metadata;
        tantivy.index_config.index_type = IndexType::Tantivy;
        tantivy.index_config.index_id = "otel-metrics-lookalike".into();
        tantivy.index_uid = IndexUid::for_test("otel-metrics-lookalike", 0);
        assert!(ParquetSplits::from_index_metadata(metastore, &tantivy).is_err());
    }
}

#[tokio::test]
async fn malformed_batches_and_queries_make_no_rpc() {
    let catalog = mocked(MockMetastoreService::new());
    let valid = split(&catalog, "one");
    let mut other_kind = split(&catalog, "two");
    other_kind.kind = ParquetSplitKind::Sketches;
    assert!(matches!(
        catalog.stage(&[valid.clone(), other_kind]).await,
        Err(MetastoreError::InvalidArgument { .. })
    ));
    let mut other_index = valid.clone();
    other_index.index_uid = IndexUid::for_test("cpu", 1).to_string();
    assert!(catalog.stage(&[other_index]).await.is_err());
    assert!(catalog.stage(&[valid.clone(), valid]).await.is_err());
    let mut query = ListParquetSplitsQuery::for_index(IndexUid::for_test("other", 0));
    assert!(catalog.list_page(&mut query).await.is_err());
    assert!(
        catalog
            .publish(&ParquetPublication {
                staged_split_ids: vec!["same".into()],
                replaced_split_ids: vec!["same".into()],
                ..Default::default()
            })
            .await
            .is_err()
    );
    catalog.stage(&[]).await.unwrap();
    catalog.mark_for_deletion(&[]).await.unwrap();
    catalog.delete(&[]).await.unwrap();
}

#[tokio::test]
async fn empty_sketch_publication_preserves_checkpoint_and_token() {
    let delta = IndexCheckpointDelta {
        source_id: "source".into(),
        source_delta: SourceCheckpointDelta::from_range(0..10),
    };
    let expected_delta = delta.clone();
    let mut mock = MockMetastoreService::new();
    mock.expect_publish_sketch_splits()
        .times(1)
        .withf(move |request| {
            request.index_uid() == &IndexUid::for_test("cpu", 0)
                && request.staged_split_ids.is_empty()
                && request.replaced_split_ids.is_empty()
                && request.deserialize_index_checkpoint().unwrap() == Some(expected_delta.clone())
                && request.publish_token_opt.as_deref() == Some("token")
        })
        .returning(|_| Ok(EmptyResponse {}));
    let catalog = ParquetSplits::new(
        MetastoreServiceClient::from_mock(mock),
        IndexUid::for_test("cpu", 0),
        ParquetSplitKind::Sketches,
    );
    catalog
        .publish(&ParquetPublication {
            checkpoint_delta: Some(delta),
            publish_token: Some("token".into()),
            ..Default::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn full_page_preserves_filters_and_advances_cursor() {
    let template = mocked(MockMetastoreService::new());
    let records: Vec<_> = (0..PARQUET_SPLITS_PAGE_SIZE)
        .map(|ordinal| record(&template, &format!("split-{ordinal:04}")))
        .collect();
    let mut mock = MockMetastoreService::new();
    let mut sequence = mockall::Sequence::new();
    mock.expect_list_metrics_splits()
        .times(1)
        .in_sequence(&mut sequence)
        .withf(|request| {
            let query = request.deserialize_query().unwrap();
            query.limit == Some(PARQUET_SPLITS_PAGE_SIZE)
                && query.after_split_id.as_deref() == Some("before")
                && query.metric_names == ["cpu"]
                && query.max_time_range_end == Some(2000)
        })
        .return_once(move |_| ListMetricsSplitsResponse::try_from_splits(&records));
    mock.expect_list_metrics_splits()
        .times(1)
        .in_sequence(&mut sequence)
        .withf(|request| {
            let query = request.deserialize_query().unwrap();
            query.after_split_id.as_deref() == Some("split-0499")
                && query.metric_names == ["cpu"]
                && query.max_time_range_end == Some(2000)
        })
        .return_once(|_| Ok(ListMetricsSplitsResponse::empty()));
    let catalog = mocked(mock);
    let result = catalog
        .list_all(
            catalog
                .query()
                .with_after_split_id("before")
                .with_metric_names(vec!["cpu".into()])
                .with_max_time_range_end(2000),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), PARQUET_SPLITS_PAGE_SIZE);
}

#[tokio::test]
async fn invalid_pages_never_advance_cursor() {
    let template = mocked(MockMetastoreService::new());
    let valid = record(&template, "split-1");
    let mut wrong_index = valid.clone();
    wrong_index.metadata.index_uid = IndexUid::for_test("other", 0).to_string();
    let mut wrong_kind = valid.clone();
    wrong_kind.metadata.kind = ParquetSplitKind::Sketches;
    let mut wrong_state = valid.clone();
    wrong_state.state = SplitState::Staged;
    for records in [
        vec![wrong_index],
        vec![wrong_kind],
        vec![wrong_state],
        vec![valid.clone(), valid.clone()],
        vec![record(&template, "split-2"), valid.clone()],
        vec![record(&template, "before")],
        vec![valid; PARQUET_SPLITS_PAGE_SIZE + 1],
    ] {
        let mut mock = MockMetastoreService::new();
        mock.expect_list_metrics_splits()
            .times(1)
            .return_once(move |_| ListMetricsSplitsResponse::try_from_splits(&records));
        let catalog = mocked(mock);
        let mut query = catalog.query().with_after_split_id("before");
        assert!(matches!(
            catalog.list_page(&mut query).await,
            Err(MetastoreError::Internal { .. })
        ));
        assert_eq!(query.after_split_id.as_deref(), Some("before"));
    }
}

#[tokio::test]
async fn transport_errors_propagate_without_retry_or_cursor_change() {
    let mut mock = MockMetastoreService::new();
    mock.expect_list_metrics_splits()
        .times(1)
        .return_once(|_| Err(MetastoreError::Unavailable("offline".to_string())));
    mock.expect_publish_metrics_splits()
        .times(1)
        .return_once(|_| {
            Err(MetastoreError::InvalidPublishToken {
                queue_id: "queue".to_string(),
            })
        });
    let catalog = mocked(mock);
    let mut query = catalog.query().with_after_split_id("before");
    assert!(matches!(
        catalog.list_page(&mut query).await,
        Err(MetastoreError::Unavailable(_))
    ));
    assert_eq!(query.after_split_id.as_deref(), Some("before"));
    assert!(matches!(
        catalog.publish(&ParquetPublication::default()).await,
        Err(MetastoreError::InvalidPublishToken { .. })
    ));
}
