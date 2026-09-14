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

//! One lifecycle for indexing and merge graphs, independent of the storage engine.
//!
//! Engines build typed actor graphs and report counters. This supervisor owns generation
//! teardown, health checks, retries and terminal states. A graph is installed only after a
//! successful spawn; failed or cancelled construction cannot leave untracked actors behind.

mod actors;
mod source;
#[cfg(test)]
mod tests;

use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Health};
use quickwit_proto::metastore::MetastoreError;
use quickwit_proto::types::IndexUid;
use serde::Serialize;
use tokio::sync::Semaphore;
use tracing::{error, info};

pub use self::actors::PipelineActors;
pub use self::source::{SourcePipeline, SourceState};
use crate::models::{IndexingStatistics, MergeStatistics};

/// Node-wide construction budgets, shared across storage engines. See issue #1638.
pub(crate) static INDEXING_SPAWN_SEMAPHORE: Semaphore = Semaphore::const_new(10);
pub(crate) static MERGE_SPAWN_SEMAPHORE: Semaphore = Semaphore::const_new(10);
const SUPERVISE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct SuperviseLoop;

#[derive(Clone, Copy, Debug, Default)]
struct Spawn {
    retry_count: usize,
}

/// Exponential backoff in seconds, capped at ten minutes without overflow.
pub(crate) fn wait_duration_before_retry(retry_count: usize) -> Duration {
    Duration::from_secs(1u64 << retry_count.min(10)).min(Duration::from_mins(10))
}

/// The engine-specific part of a pipeline. `Running` retains only the typed handles needed
/// for observations and commands; `PipelineActors` independently owns every spawned stage.
#[async_trait]
pub trait Pipeline: Send + Sync + Sized + 'static {
    type Statistics: PipelineStatistics;
    type Running: Send + Sync;
    const NAME: &'static str;

    fn index_uid(&self) -> &IndexUid;
    fn spawn_semaphore(&self) -> &'static Semaphore;
    fn restart_delay(&self) -> Duration;

    async fn spawn(
        &mut self,
        ctx: &ActorContext<PipelineSupervisor<Self>>,
        actors: &mut PipelineActors,
    ) -> anyhow::Result<Self::Running>;

    /// Add this generation's counters to the completed generations' statistics.
    fn observe(&self, running: &Self::Running, previous: Self::Statistics) -> Self::Statistics;

    /// Dynamic metadata (e.g. shard assignments) must be visible even while waiting to respawn.
    fn update_metadata(&self, _statistics: &mut Self::Statistics) {}
}

/// Only merge graphs implement this capability. Draining disables restarts before breaking
/// feedback and asking the planner to finalize; it never cancels an in-flight merge.
#[async_trait]
pub trait DrainablePipeline: Pipeline {
    async fn drain(&self, running: &Self::Running) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct FinishPendingMergesAndShutdownPipeline;

/// Common lifecycle fields in the externally observable statistics.
pub trait PipelineStatistics:
    std::fmt::Debug + Clone + Default + Serialize + Send + Sync + 'static
{
    fn set_lifecycle(&mut self, generation: usize, spawn_attempts: usize);
}

impl PipelineStatistics for IndexingStatistics {
    fn set_lifecycle(&mut self, generation: usize, spawn_attempts: usize) {
        self.generation = generation;
        self.num_spawn_attempts = spawn_attempts;
    }
}

impl PipelineStatistics for MergeStatistics {
    fn set_lifecycle(&mut self, generation: usize, spawn_attempts: usize) {
        self.generation = generation;
        self.num_spawn_attempts = spawn_attempts;
    }
}

struct Generation<P: Pipeline> {
    actors: PipelineActors,
    running: P::Running,
}

pub struct PipelineSupervisor<P: Pipeline> {
    pub(super) pipeline: P,
    running: Option<Generation<P>>,
    previous: P::Statistics,
    statistics: P::Statistics,
    generation: usize,
    spawn_attempts: usize,
    draining: bool,
}

impl<P: Pipeline> PipelineSupervisor<P> {
    pub fn from_pipeline(pipeline: P) -> Self {
        let mut statistics = P::Statistics::default();
        pipeline.update_metadata(&mut statistics);
        Self {
            pipeline,
            running: None,
            previous: P::Statistics::default(),
            statistics,
            generation: 0,
            spawn_attempts: 0,
            draining: false,
        }
    }

    fn observe(&mut self) {
        if let Some(generation) = &self.running {
            self.statistics = self
                .pipeline
                .observe(&generation.running, self.previous.clone());
        }
        self.statistics
            .set_lifecycle(self.generation, self.spawn_attempts);
        self.pipeline.update_metadata(&mut self.statistics);
    }

    async fn stop(&mut self, ctx: &ActorContext<Self>) {
        if let Some(generation) = &self.running {
            ctx.protect_future(generation.actors.stop()).await;
        }
        // Include final observations after all actors have finished their finalizers.
        self.observe();
        self.previous = self.statistics.clone();
        self.running = None;
    }
}

#[async_trait]
impl<P: Pipeline> Actor for PipelineSupervisor<P> {
    type ObservableState = P::Statistics;

    fn name(&self) -> String {
        P::NAME.to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.statistics.clone()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(Spawn::default(), ctx).await?;
        self.handle(SuperviseLoop, ctx).await
    }

    async fn finalize(
        &mut self,
        _status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        self.stop(ctx).await;
        ctx.observe(self);
        Ok(())
    }
}

#[async_trait]
impl<P: Pipeline> Handler<Spawn> for PipelineSupervisor<P> {
    type Reply = ();

    async fn handle(
        &mut self,
        spawn: Spawn,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.draining || self.running.is_some() {
            return Ok(());
        }
        let _permit = ctx
            .protect_future(self.pipeline.spawn_semaphore().acquire())
            .await
            .expect("pipeline spawn semaphore must remain open");
        self.spawn_attempts += 1;
        info!(pipeline = P::NAME, index_uid = %self.pipeline.index_uid(), generation = self.generation + 1, attempt = self.spawn_attempts, "spawning pipeline generation");
        let mut actors = PipelineActors::new(ctx.kill_switch().child());
        let result = self.pipeline.spawn(ctx, &mut actors).await;
        match result {
            Ok(running) => {
                if actors.is_empty() {
                    return Err(anyhow::anyhow!("pipeline spawned no actors").into());
                }
                self.generation += 1;
                actors.start_supervising();
                self.running = Some(Generation { actors, running });
            }
            Err(error) => {
                // A graph may fail after some stages have been spawned. Stop and join them
                // before retrying, rather than relying on the next generation's kill switch.
                ctx.protect_future(actors.stop()).await;
                if matches!(
                    error.downcast_ref::<MetastoreError>(),
                    Some(MetastoreError::NotFound(_))
                ) {
                    info!(pipeline = P::NAME, index_uid = %self.pipeline.index_uid(), error = ?error, "index deleted; pipeline stopping");
                    return Err(ActorExitStatus::Success);
                }
                let retry_delay = wait_duration_before_retry(spawn.retry_count + 1);
                error!(pipeline = P::NAME, index_uid = %self.pipeline.index_uid(), error = ?error, retry_count = spawn.retry_count, ?retry_delay, "pipeline spawn failed; retrying");
                ctx.schedule_self_msg(
                    retry_delay,
                    Spawn {
                        retry_count: spawn.retry_count + 1,
                    },
                );
            }
        }
        self.observe();
        Ok(())
    }
}

#[async_trait]
impl<P: Pipeline> Handler<SuperviseLoop> for PipelineSupervisor<P> {
    type Reply = ();

    async fn handle(
        &mut self,
        token: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.observe();
        ctx.observe(self);
        if let Some(generation) = &mut self.running {
            match generation.actors.health() {
                Health::Healthy => {}
                Health::Success => return Err(ActorExitStatus::Success),
                Health::FailureOrUnhealthy => {
                    self.stop(ctx).await;
                    if self.draining {
                        return Err(anyhow::anyhow!("pipeline failed while draining").into());
                    }
                    ctx.schedule_self_msg(self.pipeline.restart_delay(), Spawn::default());
                }
            }
        }
        ctx.schedule_self_msg(SUPERVISE_INTERVAL, token);
        Ok(())
    }
}

#[async_trait]
impl<P: DrainablePipeline> Handler<FinishPendingMergesAndShutdownPipeline>
    for PipelineSupervisor<P>
{
    type Reply = ();

    async fn handle(
        &mut self,
        _: FinishPendingMergesAndShutdownPipeline,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.draining {
            return Ok(());
        }
        self.draining = true;
        let Some(generation) = &self.running else {
            // In particular, do not leave a supervisor alive forever after a failed spawn.
            return Err(ActorExitStatus::Success);
        };
        ctx.protect_future(self.pipeline.drain(&generation.running))
            .await?;
        Ok(())
    }
}
