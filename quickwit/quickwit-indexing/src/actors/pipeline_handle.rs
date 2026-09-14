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

//! Type-erased indexing pipeline handles for the indexing service. The actor graphs remain
//! statically typed; erasure is confined to this service boundary.

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorExitStatus, ActorHandle, ActorState, DeferableReplyHandler, Health, Mailbox,
    Observation, SendError, Supervisable,
};
use quickwit_config::IndexType;
use quickwit_proto::indexing::IndexingPipelineId;

use crate::models::IndexingStatistics;
use crate::source::AssignShards;

/// The control surface shared by Tantivy and Parquet indexing pipelines.
#[async_trait]
pub trait PipelineHandle: Send + Sync {
    fn indexing_pipeline_id(&self) -> &IndexingPipelineId;
    fn index_type(&self) -> IndexType;
    fn state(&self) -> ActorState;
    fn refresh_observe(&self);
    fn last_observation(&self) -> IndexingStatistics;
    fn check_health(&self, check_for_progress: bool) -> Health;
    async fn send_assign_shards(&self, message: AssignShards) -> Result<(), SendError>;
    async fn observe(&self) -> Observation<IndexingStatistics>;
    async fn join(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics);
    async fn quit(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics);
    async fn kill(self: Box<Self>);
}

/// Generic wrapper that implements `PipelineHandle` for any actor with the right
/// observable state and message handlers.
pub(crate) struct ActorPipeline<A: Actor<ObservableState = IndexingStatistics>> {
    pub pipeline_id: IndexingPipelineId,
    pub index_type: IndexType,
    pub mailbox: Mailbox<A>,
    pub handle: ActorHandle<A>,
}

#[async_trait]
impl<A> PipelineHandle for ActorPipeline<A>
where A: Actor<ObservableState = IndexingStatistics> + DeferableReplyHandler<AssignShards>
{
    fn indexing_pipeline_id(&self) -> &IndexingPipelineId {
        &self.pipeline_id
    }

    fn index_type(&self) -> IndexType {
        self.index_type
    }

    fn state(&self) -> ActorState {
        self.handle.state()
    }

    fn refresh_observe(&self) {
        self.handle.refresh_observe();
    }

    fn last_observation(&self) -> IndexingStatistics {
        self.handle.last_observation().clone()
    }

    fn check_health(&self, check_for_progress: bool) -> Health {
        self.handle.check_health(check_for_progress)
    }

    async fn send_assign_shards(&self, message: AssignShards) -> Result<(), SendError> {
        self.mailbox.send_message(message).await?;
        Ok(())
    }

    async fn observe(&self) -> Observation<IndexingStatistics> {
        self.handle.observe().await
    }

    async fn join(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics) {
        self.handle.join().await
    }

    async fn quit(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics) {
        self.handle.quit().await
    }

    async fn kill(self: Box<Self>) {
        let _ = self.handle.kill().await;
    }
}
