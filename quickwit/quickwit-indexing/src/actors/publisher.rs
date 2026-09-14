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

//! Engine-independent publication actor. Engine implementations supply metastore
//! operations and typed merge feedback, not another handler or lifecycle loop.

use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use serde::Serialize;
use tracing::{error, info, instrument, warn};

use crate::models::{SharedPublishToken, SplitUpdate};
use crate::source::{SourceActor, SuggestTruncate};

#[derive(Clone, Debug, Default, Serialize)]
pub struct PublisherCounters {
    pub num_published_splits: u64,
    pub num_replace_operations: u64,
    pub num_empty_splits: u64,
}

/// The storage-specific part of publication. Split and feedback types are associated
/// with the engine: a publisher cannot accidentally handle another engine's message.
#[async_trait]
pub trait PublicationEngine: Clone + Send + Sync + 'static {
    type Split: Send + Sync + 'static;
    type MergeTask: Send + Sync + 'static;
    type NewSplits: Debug + Send + Sync + 'static;
    type Planner: Actor + Handler<Self::NewSplits>;

    fn split_id(split: &Self::Split) -> &str;
    fn validate(&self, update: &Publication<Self>) -> anyhow::Result<()>;
    async fn publish(
        &self,
        update: &Publication<Self>,
        token: Option<String>,
    ) -> MetastoreResult<()>;
    fn record_published(&self, update: &Publication<Self>);
    fn new_splits(splits: Vec<Self::Split>) -> Self::NewSplits;

    // Engine-local failpoints; ordinary engines need no lifecycle hooks here.
    fn before_publish(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn after_publish(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub type Publication<E> =
    SplitUpdate<<E as PublicationEngine>::Split, <E as PublicationEngine>::MergeTask>;

/// Cut the typed feedback edge so a merge pipeline can finish draining.
#[derive(Debug)]
pub(crate) struct DisconnectMergePlanner;

pub struct Publisher<E: PublicationEngine> {
    engine: E,
    name: &'static str,
    queue_capacity: QueueCapacity,
    merge_planner: Option<Mailbox<E::Planner>>,
    source: Option<Mailbox<SourceActor>>,
    publish_token: SharedPublishToken,
    counters: PublisherCounters,
}

impl<E: PublicationEngine> Clone for Publisher<E> {
    fn clone(&self) -> Self {
        // Mailboxes are cloneable independently of their actor types.
        Self {
            engine: self.engine.clone(),
            name: self.name,
            queue_capacity: self.queue_capacity,
            merge_planner: self.merge_planner.clone(),
            source: self.source.clone(),
            publish_token: self.publish_token.clone(),
            counters: self.counters.clone(),
        }
    }
}

impl<E: PublicationEngine> Publisher<E> {
    pub fn for_engine(
        engine: E,
        name: &'static str,
        queue_capacity: QueueCapacity,
        merge_planner: Option<Mailbox<E::Planner>>,
        source: Option<Mailbox<SourceActor>>,
        publish_token: SharedPublishToken,
    ) -> Self {
        Self {
            engine,
            name,
            queue_capacity,
            merge_planner,
            source,
            publish_token,
            counters: PublisherCounters::default(),
        }
    }

    /// Install the engine's typed feedback mailbox; no alternate-engine slot exists.
    pub fn with_merge_planner(mut self, mailbox: Mailbox<E::Planner>) -> Self {
        self.merge_planner = Some(mailbox);
        self
    }

    async fn publish_with_retry(
        &self,
        update: &Publication<E>,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        for (attempt, delay) in [
            Some(Duration::from_secs(1)),
            Some(Duration::from_secs(3)),
            None,
        ]
        .into_iter()
        .enumerate()
        {
            // Assignment updates can refresh the token while an earlier attempt is in flight.
            let token = self
                .publish_token
                .load()
                .as_deref()
                .map(|token| token.to_string());
            let Err(error) = ctx.protect_future(self.engine.publish(update, token)).await else {
                return Ok(());
            };
            if let Some(delay) = delay
                && matches!(error, MetastoreError::InvalidPublishToken { .. })
            {
                warn!(%error, attempt = attempt + 1, "metastore publish failed, retrying");
                ctx.protect_future(ctx.sleep(delay)).await;
            } else {
                return Err(anyhow::Error::from(error)
                    .context("failed to publish splits")
                    .into());
            }
        }
        unreachable!("last publish attempt always returns")
    }
}

#[async_trait]
impl<E: PublicationEngine> Actor for Publisher<E> {
    type ObservableState = PublisherCounters;
    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }
    fn name(&self) -> String {
        self.name.to_string()
    }
    fn queue_capacity(&self) -> QueueCapacity {
        self.queue_capacity
    }
}

#[async_trait]
impl<E: PublicationEngine> Handler<DisconnectMergePlanner> for Publisher<E> {
    type Reply = ();
    async fn handle(
        &mut self,
        _: DisconnectMergePlanner,
        _: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.merge_planner = None;
        Ok(())
    }
}

#[async_trait]
impl<E: PublicationEngine> Handler<Publication<E>> for Publisher<E> {
    type Reply = ();

    #[instrument(name = "publisher", parent = update.parent_span.id(), skip_all)]
    async fn handle(
        &mut self,
        update: Publication<E>,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.engine.before_publish()?;
        let split_ids: Vec<&str> = update.new_splits.iter().map(E::split_id).collect();
        let Some(guard) = update.publish_lock.acquire().await else {
            info!(?split_ids, "splits' publish lock is dead");
            return Ok(());
        };
        self.engine.validate(&update)?;
        let result = self.publish_with_retry(&update, ctx).await;
        drop(guard);
        if let Err(publish_error) = result {
            if is_invalid_publish_token(&publish_error)
                && let Some(source) = &self.source
            {
                error!(
                    ?publish_error,
                    ?split_ids,
                    "failed to publish splits, terminating source pipeline"
                );
                // Prevent the terminating source from publishing its final flush on a revoked
                // token.
                update.publish_lock.kill().await;
                let _ = ctx.send_exit_with_success(source).await;
                return Ok(());
            }
            return Err(publish_error);
        }
        self.engine.record_published(&update);
        let empty = update.new_splits.is_empty();
        let replacement = !update.replaced_split_ids.is_empty();
        let SplitUpdate {
            new_splits,
            checkpoint_delta_opt,
            merge_task,
            ..
        } = update;
        // Commit is already durable. These notifications are advisory: a recipient
        // may be shutting down, and its successor reloads checkpoints/published splits.
        if let Some(source) = &self.source
            && let Some(checkpoint) = checkpoint_delta_opt
        {
            let _ = ctx
                .send_message(
                    source,
                    SuggestTruncate(checkpoint.source_delta.get_source_checkpoint()),
                )
                .await;
        }
        if !empty && let Some(planner) = &self.merge_planner {
            let _ = ctx.send_message(planner, E::new_splits(new_splits)).await;
        }
        if empty {
            self.counters.num_empty_splits += 1;
        } else if replacement {
            self.counters.num_replace_operations += 1;
        } else {
            self.counters.num_published_splits += 1;
        }
        self.engine.after_publish()?;
        // The inventory guard and merge permit outlive both commit and feedback.
        drop(merge_task);
        Ok(())
    }
}

fn is_invalid_publish_token(error: &ActorExitStatus) -> bool {
    matches!(error, ActorExitStatus::Failure(error)
        if matches!(error.downcast_ref::<MetastoreError>(), Some(MetastoreError::InvalidPublishToken { .. })))
}
