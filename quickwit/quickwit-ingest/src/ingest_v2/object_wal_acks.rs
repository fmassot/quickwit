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

//! Ingest v3: applies shard replication positions once per WAL flush.
//!
//! A persist request records the positions it wrote under the WAL object id that will hold
//! them, then waits. When that object is durable, a single task takes the ingester lock once
//! and advances every position recorded for it (and earlier ids), then reports the id as
//! applied. Without this, every waiting persist request would take the ingester lock
//! individually after each flush: hundreds of lock hand-offs every flush interval, on the
//! same lock the persist path needs.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use quickwit_proto::types::{Position, QueueId};
use tokio::sync::watch;
use tracing::error;

use super::state::WeakIngesterState;
use crate::ingest_v3::ObjectWal;
use crate::ingest_v3::wal::writer::Durability;
use crate::ingest_v3::wal::{WalError, WalId, WalResult};

type PendingPositions = BTreeMap<WalId, Vec<(QueueId, Position)>>;

/// Records positions per WAL object and applies them when the object is durable.
#[derive(Clone)]
pub(super) struct ObjectWalAckApplier {
    pending: Arc<Mutex<PendingPositions>>,
    applied_rx: watch::Receiver<Durability>,
}

impl ObjectWalAckApplier {
    /// Spawns the applier task on the current runtime.
    pub fn spawn(object_wal: ObjectWal, state: WeakIngesterState) -> Self {
        let pending: Arc<Mutex<PendingPositions>> = Arc::new(Mutex::new(BTreeMap::new()));
        let initial = object_wal.durability_receiver().borrow().clone();
        let (applied_tx, applied_rx) = watch::channel(initial);
        let task = ApplierTask {
            pending: pending.clone(),
            durability_rx: object_wal.durability_receiver(),
            applied_tx,
            state,
        };
        tokio::spawn(task.run());
        Self {
            pending,
            applied_rx,
        }
    }

    /// Records that `positions` become the replication positions of their shards once `wal_id`
    /// is durable.
    pub fn record(&self, wal_id: WalId, positions: Vec<(QueueId, Position)>) {
        self.pending
            .lock()
            .unwrap()
            .entry(wal_id)
            .or_default()
            .extend(positions);
    }

    /// Resolves once `wal_id` is durable *and* its positions have been applied, or with the
    /// terminal error of the writer.
    pub async fn wait_applied(&self, wal_id: WalId) -> WalResult<()> {
        let mut rx = self.applied_rx.clone();
        loop {
            match &*rx.borrow_and_update() {
                Durability::Flushed(applied) if *applied >= wal_id => return Ok(()),
                Durability::Failed(error) => return Err(error.clone()),
                Durability::Flushed(_) => {}
            }
            if rx.changed().await.is_err() {
                return Err(WalError::Closed);
            }
        }
    }
}

struct ApplierTask {
    pending: Arc<Mutex<PendingPositions>>,
    durability_rx: watch::Receiver<Durability>,
    applied_tx: watch::Sender<Durability>,
    state: WeakIngesterState,
}

impl ApplierTask {
    async fn run(mut self) {
        loop {
            let durability = self.durability_rx.borrow_and_update().clone();
            match durability {
                Durability::Flushed(durable_wal_id) => {
                    let positions = self.drain_up_to(durable_wal_id);
                    if !positions.is_empty() && !self.apply(positions).await {
                        // The ingester is gone.
                        return;
                    }
                    let _ = self.applied_tx.send(Durability::Flushed(durable_wal_id));
                }
                Durability::Failed(error) => {
                    self.pending.lock().unwrap().clear();
                    let _ = self.applied_tx.send(Durability::Failed(error));
                    return;
                }
            }
            if self.durability_rx.changed().await.is_err() {
                return;
            }
        }
    }

    fn drain_up_to(&self, durable_wal_id: WalId) -> Vec<(QueueId, Position)> {
        let mut pending = self.pending.lock().unwrap();
        let later = pending.split_off(&durable_wal_id.next());
        let drained = std::mem::replace(&mut *pending, later);
        drained.into_values().flatten().collect()
    }

    /// Takes the ingester lock once and advances the positions. Returns `false` if the
    /// ingester state is gone.
    async fn apply(&self, positions: Vec<(QueueId, Position)>) -> bool {
        let Some(state) = self.state.upgrade() else {
            return false;
        };
        let mut state_guard = match state.lock_partially("persist_ack").await {
            Ok(guard) => guard,
            Err(error) => {
                error!(%error, "failed to lock the ingester state to apply positions");
                return true;
            }
        };
        let now = Instant::now();
        for (queue_id, position) in positions {
            if let Some(shard) = state_guard.shards.get_mut(&queue_id) {
                shard.set_replication_position_inclusive(position, now);
            }
        }
        true
    }
}
