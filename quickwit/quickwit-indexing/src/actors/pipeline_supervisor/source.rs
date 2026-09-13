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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorExitStatus, ActorHandle, Handler, Inbox, Mailbox};
use quickwit_metrics::{GaugeGuard, gauge};
use quickwit_proto::indexing::IndexingPipelineId;
use tracing::warn;

use super::{Pipeline, PipelineActors, PipelineSupervisor};
use crate::metrics::INDEXING_PIPELINES;
use crate::models::{IndexingStatistics, SharedPublishToken};
use crate::source::{
    AssignShards, Assignment, SourceActor, SourceRuntime, SourceSink, quickwit_supported_sources,
};

/// Source state belongs to the supervisor, not a generation: assignments and publish tokens
/// survive actor restarts. Both storage engines use exactly the same assignment semantics.
pub struct SourceState {
    assignment: Assignment,
    pub publish_token: SharedPublishToken,
    params_fingerprint: u64,
    _gauge: GaugeGuard,
}

impl SourceState {
    pub fn new(pipeline_id: &IndexingPipelineId, params_fingerprint: u64) -> Self {
        let gauge =
            gauge!(parent: INDEXING_PIPELINES, "index" => pipeline_id.index_uid.index_id.clone());
        Self {
            assignment: Assignment {
                shard_ids: Default::default(),
                indexing_plan_id: String::new(),
            },
            publish_token: SharedPublishToken::default(),
            params_fingerprint,
            _gauge: GaugeGuard::new(&gauge, 1.0),
        }
    }

    pub fn update_metadata(&self, statistics: &mut IndexingStatistics) {
        statistics.shard_ids.clone_from(&self.assignment.shard_ids);
        statistics.params_fingerprint = self.params_fingerprint;
    }

    pub async fn spawn<P: Pipeline>(
        &self,
        ctx: &ActorContext<PipelineSupervisor<P>>,
        actors: &mut PipelineActors,
        mut runtime: SourceRuntime,
        sink: impl Into<SourceSink>,
        mailboxes: (Mailbox<SourceActor>, Inbox<SourceActor>),
    ) -> anyhow::Result<Arc<ActorHandle<SourceActor>>> {
        runtime.publish_token = self.publish_token.clone();
        let source = ctx
            .protect_future(quickwit_supported_sources().load_source(runtime))
            .await?;
        let (mailbox, handle) = actors.spawn(
            ctx.spawn_actor().set_mailboxes(mailboxes.0, mailboxes.1),
            SourceActor::new(source, sink),
        );
        mailbox
            .send_message(AssignShards(self.assignment.clone()))
            .await?;
        Ok(handle)
    }
}

/// Only indexing graphs accept shard assignments; merge graphs do not implement this trait.
pub trait SourcePipeline: Pipeline<Statistics = IndexingStatistics> {
    fn source(&mut self) -> &mut SourceState;
    fn source_mailbox(running: &Self::Running) -> &Mailbox<SourceActor>;
}

#[async_trait]
impl<P: SourcePipeline> Handler<AssignShards> for PipelineSupervisor<P> {
    type Reply = ();

    async fn handle(
        &mut self,
        message: AssignShards,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.pipeline.source().assignment = message.0.clone();
        if let Some(generation) = &self.running
            && let Err(error) = P::source_mailbox(&generation.running)
                .send_message(message)
                .await
        {
            // A dead source is handled by supervision. The saved assignment is replayed on
            // respawn; failing the supervisor here would lose that recovery path.
            warn!(%error, "source mailbox closed; shards will be reassigned on respawn");
        }
        self.observe();
        ctx.observe(self);
        Ok(())
    }
}
