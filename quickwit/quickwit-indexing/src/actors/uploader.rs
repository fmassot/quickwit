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

//! Shared concurrent upload actor. Engines prepare/stage/store their own artifacts;
//! reservation, backpressure, cancellation, failure propagation and task ownership
//! are implemented once. Worker tasks cannot outlive a completed actor generation.

mod delivery;
#[cfg(test)]
mod tests;

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use async_trait::async_trait;
pub use delivery::PublicationMailbox;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, QueueCapacity};
use quickwit_common::KillSwitch;
use quickwit_metrics::{gauge, label_values};
use serde::Serialize;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::JoinSet;
use tracing::{Instrument, Span};

use crate::actors::publisher::{Publication, PublicationEngine};
use crate::metrics::{AVAILABLE_CONCURRENT_UPLOAD_PERMITS, COMPONENT};
use crate::models::PublishLock;

#[derive(Clone, Copy, Debug)]
pub enum UploaderType {
    IndexUploader,
    MergeUploader,
    DeleteUploader,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UploaderCounters {
    pub num_staged_splits: Arc<AtomicU64>,
    pub num_uploaded_splits: Arc<AtomicU64>,
}

/// Separate ingest/merge capacity per engine, preserving existing node-wide budgets.
/// Engines select labels/capacity; semaphore handling is shared.
pub struct UploadBudget {
    indexing: OnceLock<Semaphore>,
    merging: OnceLock<Semaphore>,
    indexing_label: &'static str,
    merging_label: &'static str,
}
impl UploadBudget {
    pub const fn new(indexing_label: &'static str, merging_label: &'static str) -> Self {
        Self {
            indexing: OnceLock::new(),
            merging: OnceLock::new(),
            indexing_label,
            merging_label,
        }
    }
    async fn acquire(
        &'static self,
        role: UploaderType,
        maximum: usize,
    ) -> anyhow::Result<SemaphorePermit<'static>> {
        let (slot, label) = match role {
            UploaderType::IndexUploader => (&self.indexing, self.indexing_label),
            UploaderType::MergeUploader | UploaderType::DeleteUploader => {
                (&self.merging, self.merging_label)
            }
        };
        let semaphore = slot.get_or_init(|| Semaphore::const_new(maximum));
        gauge!(parent: AVAILABLE_CONCURRENT_UPLOAD_PERMITS, labels: [label_values!(COMPONENT => label)])
            .set(semaphore.available_permits() as f64);
        semaphore
            .acquire()
            .await
            .context("upload semaphore closed unexpectedly")
    }
}

#[async_trait]
pub trait UploadEngine: Clone + Send + Sync + 'static {
    type Publisher: PublicationEngine;
    type Batch: Debug + Send + 'static;
    type Prepared: Send + Sync + 'static;
    const QUEUE_CAPACITY: usize;
    fn actor_name(role: UploaderType) -> String;
    fn budget() -> &'static UploadBudget;
    fn publish_lock(batch: &Self::Batch) -> &PublishLock;
    fn prepare(&self, batch: Self::Batch) -> anyhow::Result<Self::Prepared>;
    fn split_count(prepared: &Self::Prepared) -> usize;
    async fn stage(&self, prepared: &Self::Prepared) -> anyhow::Result<()>;
    /// Keep prepared artifacts/scratch ownership until all files are stored. Transfer
    /// merge-task ownership to the returned publication; count each successful upload.
    async fn upload(
        &self,
        prepared: Self::Prepared,
        counters: &UploaderCounters,
    ) -> anyhow::Result<Publication<Self::Publisher>>;
}

pub struct Uploader<E: UploadEngine> {
    engine: E,
    role: UploaderType,
    destination: PublicationMailbox<E::Publisher>,
    maximum: usize,
    counters: UploaderCounters,
    tasks: JoinSet<anyhow::Result<()>>,
}

impl<E: UploadEngine> Clone for Uploader<E> {
    fn clone(&self) -> Self {
        // The actor framework clones an unstarted template for per-actor supervision
        // (notably the Tantivy delete pipeline). Never copy a running worker set.
        assert!(
            self.tasks.is_empty(),
            "cannot clone an uploader with outstanding workers"
        );
        Self {
            engine: self.engine.clone(),
            role: self.role,
            destination: self.destination.clone(),
            maximum: self.maximum,
            counters: self.counters.clone(),
            tasks: JoinSet::new(),
        }
    }
}

impl<E: UploadEngine> Uploader<E> {
    pub fn for_engine(
        engine: E,
        role: UploaderType,
        destination: PublicationMailbox<E::Publisher>,
        maximum: usize,
    ) -> Self {
        Self {
            engine,
            role,
            destination,
            maximum,
            counters: UploaderCounters::default(),
            tasks: JoinSet::new(),
        }
    }

    pub(crate) async fn forward(
        &self,
        update: Publication<E::Publisher>,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        self.destination.reserve(ctx).await?.send(update, ctx).await
    }

    /// The small engine-specific message handlers delegate here; they do not own
    /// concurrent tasks or duplicate upload lifecycle behavior.
    pub(crate) async fn upload(
        &mut self,
        batch: E::Batch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        while let Some(result) = self.tasks.try_join_next() {
            result.map_err(anyhow::Error::from)??;
        }
        let sender = self.destination.reserve(ctx).await?;
        let permit = ctx
            .protect_future(E::budget().acquire(self.role, self.maximum))
            .await?;
        if ctx.kill_switch().is_dead() {
            return Err(ActorExitStatus::Killed);
        }
        let engine = self.engine.clone();
        let publish_lock = E::publish_lock(&batch).clone();
        let counters = self.counters.clone();
        let ctx = ctx.clone();
        self.tasks.spawn(
            async move {
                let mut guard = UploadTaskGuard {
                    kill_switch: ctx.kill_switch().clone(),
                    completed: false,
                };
                let result: anyhow::Result<()> = async {
                    if publish_lock.is_dead() {
                        return sender.discard();
                    }
                    let prepared = engine.prepare(batch)?;
                    engine.stage(&prepared).await?;
                    counters
                        .num_staged_splits
                        .fetch_add(E::split_count(&prepared) as u64, Ordering::Relaxed);
                    let publication = engine.upload(prepared, &counters).await?;
                    sender.send(publication, &ctx).await
                }
                .await;
                if let Err(error) = &result {
                    guard
                        .kill_switch
                        .kill_with_fault(anyhow::anyhow!("upload failed: {error:#}"));
                }
                guard.completed = true;
                drop(permit);
                result
            }
            .instrument(Span::current()),
        );
        Ok(())
    }
}

#[async_trait]
impl<E: UploadEngine> Actor for Uploader<E> {
    type ObservableState = UploaderCounters;
    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }
    fn name(&self) -> String {
        E::actor_name(self.role)
    }
    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(E::QUEUE_CAPACITY)
    }

    async fn finalize(
        &mut self,
        status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        let _guard = ctx.protect_zone();
        let mut cancelling = !status.is_success();
        if cancelling {
            self.tasks.abort_all();
        }
        let mut failure = None;
        let mut panicked = false;
        while let Some(result) = self.tasks.join_next().await {
            let error = match result {
                Ok(Ok(())) => continue,
                Ok(Err(error)) => error,
                Err(error) if cancelling && error.is_cancelled() => continue,
                Err(error) => {
                    panicked |= error.is_panic();
                    anyhow::Error::from(error)
                }
            };
            failure.get_or_insert(error);
            cancelling = true;
            self.tasks.abort_all();
        }
        if let Some(error) = failure {
            // A worker fault already killed this generation. Preserve that failed status,
            // rather than reporting an ordinary I/O error as a finalizer panic. A failure
            // discovered while draining a successful exit must still make the actor fail.
            if status.is_success() || panicked {
                return Err(error);
            }
            tracing::error!(%error, "upload failed while stopping a failed generation");
        }
        Ok(())
    }
}

/// A panic/cancellation must fault the generation too, not just drop a sequencer
/// slot silently (direct-to-publisher merge pipelines have no sequencer to detect it).
struct UploadTaskGuard {
    kill_switch: KillSwitch,
    completed: bool,
}
impl Drop for UploadTaskGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.kill_switch
                .kill_with_fault(anyhow::anyhow!("upload task ended before completion"));
        }
    }
}
