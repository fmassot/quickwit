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

//! Periodic garbage collection of the ingest v3 object-store WAL.
//!
//! See `quickwit_ingest::ingest_v3::gc` for what gets deleted and why. This actor only
//! provides the schedule and the metastore as the source of publish positions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_ingest::ingest_v3::gc::{
    FooterCache, IngestWalGcParams, IngestWalGcReport, PublishedPositions,
    collect_ingest_wal_garbage,
};
use quickwit_ingest::ingest_v3::metrics::{
    WAL_OBJECT_GC_DELETED_BY_JANITOR, WAL_OBJECT_GC_DELETED_BYTES_BY_JANITOR_TOTAL,
    WAL_OBJECT_LIVE_OBJECTS,
};
use quickwit_proto::metastore::{
    ListShardsRequest, ListShardsSubrequest, MetastoreError, MetastoreService,
    MetastoreServiceClient,
};
use quickwit_proto::types::{IndexUid, Position, ShardId, SourceId};
use quickwit_storage::Storage;
use serde::Serialize;
use tracing::{error, info};

const RUN_INTERVAL: Duration = Duration::from_mins(10);

#[derive(Debug)]
struct Loop;

/// Publish positions as recorded by the metastore.
struct MetastorePublishedPositions {
    metastore: MetastoreServiceClient,
}

#[async_trait]
impl PublishedPositions for MetastorePublishedPositions {
    async fn list_shard_positions(
        &self,
        index_uid: &IndexUid,
        source_id: &SourceId,
    ) -> anyhow::Result<Option<HashMap<ShardId, Position>>> {
        let request = ListShardsRequest {
            subrequests: vec![ListShardsSubrequest {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_state: None,
            }],
        };
        match self.metastore.list_shards(request).await {
            Ok(response) => {
                let mut positions = HashMap::new();
                for subresponse in response.subresponses {
                    for shard in subresponse.shards {
                        positions
                            .insert(shard.shard_id().clone(), shard.publish_position_inclusive());
                    }
                }
                Ok(Some(positions))
            }
            // The index or the source is gone: nothing will ever read those records.
            Err(MetastoreError::NotFound(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct IngestWalGarbageCollectorCounters {
    pub num_passes: usize,
    pub num_failed_passes: usize,
    pub num_deleted_objects: usize,
    pub num_deleted_bytes: u64,
    pub num_failed_deletions: usize,
}

/// Runs [`collect_ingest_wal_garbage`] every [`RUN_INTERVAL`].
pub struct IngestWalGarbageCollector {
    wal_storage: Arc<dyn Storage>,
    positions: MetastorePublishedPositions,
    params: IngestWalGcParams,
    footer_cache: FooterCache,
    counters: IngestWalGarbageCollectorCounters,
}

impl IngestWalGarbageCollector {
    pub fn new(
        wal_storage: Arc<dyn Storage>,
        metastore: MetastoreServiceClient,
        min_age: Duration,
    ) -> Self {
        Self {
            wal_storage,
            positions: MetastorePublishedPositions { metastore },
            params: IngestWalGcParams {
                min_age,
                ..IngestWalGcParams::default()
            },
            footer_cache: FooterCache::new(),
            counters: IngestWalGarbageCollectorCounters::default(),
        }
    }

    fn record(&mut self, report: &IngestWalGcReport) {
        self.counters.num_deleted_objects += report.num_deleted_objects;
        self.counters.num_deleted_bytes += report.num_deleted_bytes;
        self.counters.num_failed_deletions += report.num_failed_deletions;
        WAL_OBJECT_GC_DELETED_BY_JANITOR.inc_by(report.num_deleted_objects as u64);
        WAL_OBJECT_GC_DELETED_BYTES_BY_JANITOR_TOTAL.inc_by(report.num_deleted_bytes);
        WAL_OBJECT_LIVE_OBJECTS.set((report.num_objects - report.num_deleted_objects) as f64);
        if report.num_deleted_objects > 0 || report.num_failed_deletions > 0 {
            info!(
                num_logs = report.num_logs,
                num_objects = report.num_objects,
                num_deleted_objects = report.num_deleted_objects,
                num_deleted_bytes = report.num_deleted_bytes,
                num_skipped_objects = report.num_skipped_objects,
                num_failed_deletions = report.num_failed_deletions,
                "ingest WAL garbage collection pass"
            );
        }
    }
}

#[async_trait]
impl Actor for IngestWalGarbageCollector {
    type ObservableState = IngestWalGarbageCollectorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        "IngestWalGarbageCollector".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(Loop, ctx).await
    }
}

#[async_trait]
impl Handler<Loop> for IngestWalGarbageCollector {
    type Reply = ();

    async fn handle(&mut self, _: Loop, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.counters.num_passes += 1;
        let result = ctx
            .protect_future(collect_ingest_wal_garbage(
                self.wal_storage.clone(),
                &self.positions,
                &self.params,
                &mut self.footer_cache,
                SystemTime::now(),
            ))
            .await;
        match result {
            Ok(report) => self.record(&report),
            Err(error) => {
                self.counters.num_failed_passes += 1;
                error!(%error, "ingest WAL garbage collection failed");
            }
        }
        ctx.schedule_self_msg(RUN_INTERVAL, Loop);
        Ok(())
    }
}
