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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use quickwit_actors::{ActorHandle, Mailbox, Universe};
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_proto::metastore::EntityKind;

use super::*;

#[derive(Default)]
struct Probe {
    attempts: AtomicUsize,
    live: AtomicUsize,
    stages: Mutex<Vec<Mailbox<Stage>>>,
}

struct Stage(Arc<Probe>);

impl Stage {
    fn new(probe: Arc<Probe>) -> Self {
        probe.live.fetch_add(1, Ordering::SeqCst);
        Self(probe)
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Actor for Stage {
    type ObservableState = ();
    fn observable_state(&self) {}
}

#[derive(Debug)]
struct Finish;
#[derive(Debug)]
struct Fail;

#[async_trait]
impl Handler<Finish> for Stage {
    type Reply = ();
    async fn handle(&mut self, _: Finish, _: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        Err(ActorExitStatus::Success)
    }
}

#[async_trait]
impl Handler<Fail> for Stage {
    type Reply = ();
    async fn handle(&mut self, _: Fail, _: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        Err(anyhow::anyhow!("injected stage failure").into())
    }
}

struct TestGraph {
    index_uid: IndexUid,
    probe: Arc<Probe>,
    fail_spawns: usize,
    deleted: bool,
    fail_drain: bool,
}

impl TestGraph {
    fn new(probe: &Arc<Probe>) -> Self {
        Self {
            index_uid: IndexUid::for_test("test-supervisor", 0),
            probe: probe.clone(),
            fail_spawns: 0,
            deleted: false,
            fail_drain: false,
        }
    }
}

#[async_trait]
impl Pipeline for TestGraph {
    type Statistics = MergeStatistics;
    type Running = Vec<Arc<ActorHandle<Stage>>>;
    const NAME: &'static str = "TestPipeline";

    fn index_uid(&self) -> &IndexUid {
        &self.index_uid
    }

    fn spawn_semaphore(&self) -> &'static Semaphore {
        &MERGE_SPAWN_SEMAPHORE
    }
    fn restart_delay(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn spawn(
        &mut self,
        ctx: &ActorContext<PipelineSupervisor<Self>>,
        actors: &mut PipelineActors,
    ) -> anyhow::Result<Self::Running> {
        // A new generation must not overlap actors from a failed or partially spawned graph.
        anyhow::ensure!(
            self.probe.live.load(Ordering::SeqCst) == 0,
            "previous generation still alive"
        );
        let attempt = self.probe.attempts.fetch_add(1, Ordering::SeqCst);
        let mut stages = Vec::new();
        for _ in 0..2 {
            let (mailbox, handle) = actors.spawn(ctx.spawn_actor(), Stage::new(self.probe.clone()));
            self.probe.stages.lock().unwrap().push(mailbox);
            stages.push(handle);
        }
        if self.deleted {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: "deleted".to_string(),
            })
            .into());
        }
        if attempt < self.fail_spawns {
            anyhow::bail!("injected partial spawn failure");
        }
        Ok(stages)
    }

    fn observe(&self, _: &Self::Running, mut previous: MergeStatistics) -> MergeStatistics {
        previous.num_uploaded_splits += 1;
        previous
    }
}

#[async_trait]
impl DrainablePipeline for TestGraph {
    async fn drain(&self, stages: &Self::Running) -> anyhow::Result<()> {
        if self.fail_drain {
            stages[0].mailbox().send_message(Fail).await?;
        } else {
            for stage in stages {
                stage.mailbox().send_message(Finish).await?;
            }
        }
        Ok(())
    }
}

async fn wait_for_attempts(probe: &Arc<Probe>, count: usize) {
    wait_until_predicate(
        || async { probe.attempts.load(Ordering::SeqCst) >= count },
        Duration::from_secs(10),
        Duration::from_millis(10),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn partial_spawn_is_cleaned_up_before_retry() {
    let universe = Universe::with_accelerated_time();
    let probe = Arc::new(Probe::default());
    let mut graph = TestGraph::new(&probe);
    graph.fail_spawns = 1;
    let (_, handle) = universe
        .spawn_builder()
        .spawn(PipelineSupervisor::from_pipeline(graph));
    wait_for_attempts(&probe, 2).await;
    let stats = handle.process_pending_and_observe().await;
    assert_eq!(stats.generation, 1);
    assert_eq!(stats.num_spawn_attempts, 2);
    assert_eq!(probe.live.load(Ordering::SeqCst), 2);
    handle.quit().await;
    assert_eq!(probe.live.load(Ordering::SeqCst), 0);
    universe.assert_quit().await;
}

#[tokio::test]
async fn every_registered_stage_is_supervised_and_counters_accumulate() {
    let universe = Universe::with_accelerated_time();
    let probe = Arc::new(Probe::default());
    let (_, handle) = universe
        .spawn_builder()
        .spawn(PipelineSupervisor::from_pipeline(TestGraph::new(&probe)));
    handle.process_pending_and_observe().await;
    // Fail the second stage: no separate list of "supervisable" handles should be necessary.
    let stage = probe.stages.lock().unwrap()[1].clone();
    stage.send_message(Fail).await.unwrap();
    wait_for_attempts(&probe, 2).await;
    let stats = handle.process_pending_and_observe().await;
    assert_eq!(stats.generation, 2);
    assert_eq!(stats.num_spawn_attempts, 2);
    assert_eq!(stats.num_uploaded_splits, 2);
    // Re-observing must not add this generation twice.
    assert_eq!(
        handle
            .process_pending_and_observe()
            .await
            .num_uploaded_splits,
        2
    );
    handle.quit().await;
    assert_eq!(probe.live.load(Ordering::SeqCst), 0);
    universe.assert_quit().await;
}

#[tokio::test]
async fn duplicate_spawn_does_not_start_another_generation() {
    let universe = Universe::with_accelerated_time();
    let probe = Arc::new(Probe::default());
    let (mailbox, handle) = universe
        .spawn_builder()
        .spawn(PipelineSupervisor::from_pipeline(TestGraph::new(&probe)));
    mailbox.send_message(Spawn::default()).await.unwrap();
    mailbox.send_message(Spawn::default()).await.unwrap();
    assert_eq!(handle.process_pending_and_observe().await.generation, 1);
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 1);
    handle.quit().await;
    universe.assert_quit().await;
}

#[tokio::test]
async fn deleted_index_stops_without_retry_or_leaked_stages() {
    let universe = Universe::with_accelerated_time();
    let probe = Arc::new(Probe::default());
    let mut graph = TestGraph::new(&probe);
    graph.deleted = true;
    let (_, handle) = universe
        .spawn_builder()
        .spawn(PipelineSupervisor::from_pipeline(graph));
    let (status, _) = handle.join().await;
    assert!(status.is_success());
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(probe.live.load(Ordering::SeqCst), 0);
    universe.assert_quit().await;
}

#[tokio::test]
async fn drain_while_waiting_to_retry_is_terminal() {
    let universe = Universe::with_accelerated_time();
    let probe = Arc::new(Probe::default());
    let mut graph = TestGraph::new(&probe);
    graph.fail_spawns = usize::MAX;
    let (mailbox, handle) = universe
        .spawn_builder()
        .spawn(PipelineSupervisor::from_pipeline(graph));
    mailbox
        .send_message(FinishPendingMergesAndShutdownPipeline)
        .await
        .unwrap();
    let (status, _) = handle.join().await;
    assert!(status.is_success());
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(probe.live.load(Ordering::SeqCst), 0);
    universe.assert_quit().await;
}

#[tokio::test]
async fn drain_completes_without_restarting() {
    let universe = Universe::with_accelerated_time();
    let probe = Arc::new(Probe::default());
    let (mailbox, handle) = universe
        .spawn_builder()
        .spawn(PipelineSupervisor::from_pipeline(TestGraph::new(&probe)));
    mailbox
        .send_message(FinishPendingMergesAndShutdownPipeline)
        .await
        .unwrap();
    let (status, _) = handle.join().await;
    assert!(status.is_success());
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(probe.live.load(Ordering::SeqCst), 0);
    universe.assert_quit().await;
}

#[tokio::test]
async fn failure_during_drain_is_terminal_not_success() {
    let universe = Universe::with_accelerated_time();
    let probe = Arc::new(Probe::default());
    let mut graph = TestGraph::new(&probe);
    graph.fail_drain = true;
    let (mailbox, handle) = universe
        .spawn_builder()
        .spawn(PipelineSupervisor::from_pipeline(graph));
    mailbox
        .send_message(FinishPendingMergesAndShutdownPipeline)
        .await
        .unwrap();
    let (status, _) = handle.join().await;
    assert!(!status.is_success());
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(probe.live.load(Ordering::SeqCst), 0);
    universe.assert_quit().await;
}

#[tokio::test]
async fn dropping_uninstalled_graph_kills_its_stages() {
    let universe = Universe::with_accelerated_time();
    let probe = Arc::new(Probe::default());
    let mut actors = PipelineActors::new(Default::default());
    let (_, handle) = actors.spawn(universe.spawn_builder(), Stage::new(probe.clone()));
    drop(actors);
    let (status, _) = handle.wait().await;
    assert!(!status.is_success());
    // Waiting is repeatable and retains the final observation for the supervisor.
    let (status_again, _) = handle.wait().await;
    assert_eq!(format!("{status:?}"), format!("{status_again:?}"));
    assert_eq!(probe.live.load(Ordering::SeqCst), 0);
    universe.assert_quit().await;
}
