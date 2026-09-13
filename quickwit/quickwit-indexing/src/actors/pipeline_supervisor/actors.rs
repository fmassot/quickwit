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
use std::time::Instant;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorHandle, Command, HEARTBEAT, Health, Mailbox, SpawnBuilder, Supervisable,
};
use quickwit_common::KillSwitch;
use tracing::debug;

#[async_trait]
trait ManagedActor: Send + Sync {
    fn health(&self, check_progress: bool) -> Health;
    fn name(&self) -> &str;
    async fn wait(&self);
}

#[async_trait]
impl<A: Actor> ManagedActor for Arc<ActorHandle<A>> {
    fn health(&self, check_progress: bool) -> Health {
        self.check_health(check_progress)
    }

    fn name(&self) -> &str {
        self.as_ref().name()
    }

    async fn wait(&self) {
        // Wake idle actors so they notice the generation's killed switch.
        let _ = self
            .mailbox()
            .send_message_with_high_priority(Command::Nudge);
        self.as_ref().wait().await;
    }
}

/// Owns every actor in one pipeline generation. Use `spawn` for *all* stages, including
/// sequencers. Registration and the child kill switch cannot be forgotten by an engine.
/// Dropping a partially constructed graph kills its actors, including on cancellation.
pub struct PipelineActors {
    kill_switch: KillSwitch,
    actors: Vec<Box<dyn ManagedActor>>,
    next_progress_check: Instant,
}

impl PipelineActors {
    pub(super) fn new(kill_switch: KillSwitch) -> Self {
        Self {
            kill_switch,
            actors: Vec::new(),
            next_progress_check: Instant::now() + *HEARTBEAT,
        }
    }

    pub fn spawn<A: Actor>(
        &mut self,
        builder: SpawnBuilder<A>,
        actor: A,
    ) -> (Mailbox<A>, Arc<ActorHandle<A>>) {
        let (mailbox, handle) = builder
            .set_kill_switch(self.kill_switch.clone())
            .spawn(actor);
        let handle = Arc::new(handle);
        self.actors.push(Box::new(handle.clone()));
        (mailbox, handle)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }

    pub(super) fn start_supervising(&mut self) {
        // Construction can be slow. Give even the last stage a full heartbeat after the
        // graph is installed, rather than charging spawn time against its progress budget.
        self.next_progress_check = Instant::now() + *HEARTBEAT;
    }

    pub(super) fn health(&mut self) -> Health {
        let now = Instant::now();
        let check_progress = now >= self.next_progress_check;
        if check_progress {
            self.next_progress_check = now + *HEARTBEAT;
        }
        let mut health = Health::Success;
        for actor in &self.actors {
            match actor.health(check_progress) {
                Health::FailureOrUnhealthy => {
                    debug!(
                        actor = actor.name(),
                        "pipeline actor failed or stopped progressing"
                    );
                    health = Health::FailureOrUnhealthy;
                }
                Health::Healthy if health == Health::Success => health = Health::Healthy,
                _ => {}
            }
        }
        health
    }

    pub(super) async fn stop(&self) {
        self.kill_switch.kill();
        futures::future::join_all(self.actors.iter().map(|actor| actor.wait())).await;
    }
}

impl Drop for PipelineActors {
    fn drop(&mut self) {
        self.kill_switch.kill();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;

    struct ProgressProbe(Arc<AtomicBool>);

    #[async_trait]
    impl ManagedActor for ProgressProbe {
        fn health(&self, check_progress: bool) -> Health {
            self.0.store(check_progress, Ordering::SeqCst);
            Health::Healthy
        }
        fn name(&self) -> &str {
            "progress-probe"
        }
        async fn wait(&self) {}
    }

    #[test]
    fn construction_time_does_not_consume_progress_budget() {
        let checked_progress = Arc::new(AtomicBool::new(true));
        let mut actors = PipelineActors::new(Default::default());
        actors
            .actors
            .push(Box::new(ProgressProbe(checked_progress.clone())));
        actors.next_progress_check = Instant::now() - Duration::from_secs(1);
        actors.start_supervising();
        assert_eq!(actors.health(), Health::Healthy);
        assert!(!checked_progress.load(Ordering::SeqCst));
    }
}
