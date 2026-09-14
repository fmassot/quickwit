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

use std::time::Duration;

use quickwit_actors::{Command, Handler, Universe};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::types::{IndexUid, SplitId};

use super::*;
use crate::actors::Publisher;
use crate::actors::tantivy::publisher::TantivyPublication;
use crate::models::SplitsUpdate;

static BUDGET: UploadBudget = UploadBudget::new("test-index", "test-merge");

#[derive(Clone)]
struct ProbeUpload {
    stage_failure: bool,
    upload_failure: bool,
    panic: bool,
    entered: Arc<AtomicU64>,
    released: Arc<AtomicU64>,
    gate: Arc<Semaphore>,
}

#[derive(Debug)]
struct Release(Arc<AtomicU64>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct Batch {
    lock: PublishLock,
    _resource: Release,
}
impl Default for ProbeUpload {
    fn default() -> Self {
        Self {
            stage_failure: false,
            upload_failure: false,
            panic: false,
            entered: Arc::default(),
            released: Arc::default(),
            gate: Arc::new(Semaphore::new(0)),
        }
    }
}
impl ProbeUpload {
    fn batch(&self) -> Batch {
        Batch {
            lock: PublishLock::default(),
            _resource: Release(self.released.clone()),
        }
    }
}

#[async_trait]
impl UploadEngine for ProbeUpload {
    type Publisher = TantivyPublication;
    type Batch = Batch;
    type Prepared = Batch;
    const QUEUE_CAPACITY: usize = 1;
    fn actor_name(_: UploaderType) -> String {
        "ProbeUploader".to_string()
    }
    fn budget() -> &'static UploadBudget {
        &BUDGET
    }
    fn publish_lock(batch: &Batch) -> &PublishLock {
        &batch.lock
    }
    fn prepare(&self, batch: Batch) -> anyhow::Result<Batch> {
        Ok(batch)
    }
    fn split_count(_: &Batch) -> usize {
        1
    }
    async fn stage(&self, _: &Batch) -> anyhow::Result<()> {
        anyhow::ensure!(!self.stage_failure, "injected staging failure");
        Ok(())
    }
    async fn upload(
        &self,
        batch: Batch,
        counters: &UploaderCounters,
    ) -> anyhow::Result<SplitsUpdate> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let _permit = self.gate.acquire().await?;
        assert!(!self.panic, "injected upload panic");
        anyhow::ensure!(!self.upload_failure, "injected upload failure");
        counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);
        Ok(SplitsUpdate {
            index_uid: IndexUid::for_test("test", 0),
            new_splits: vec![SplitMetadata {
                split_id: SplitId::from("split"),
                ..Default::default()
            }],
            replaced_split_ids: Vec::new(),
            checkpoint_delta_opt: None,
            publish_lock: batch.lock,
            merge_task: None,
            parent_span: Span::none(),
        })
    }
}
#[async_trait]
impl Handler<Batch> for Uploader<ProbeUpload> {
    type Reply = ();
    async fn handle(
        &mut self,
        batch: Batch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.upload(batch, ctx).await
    }
}

async fn entered(engine: &ProbeUpload) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while engine.entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upload should reach the gate");
}

#[tokio::test]
async fn cloning_a_template_never_copies_outstanding_workers() {
    let universe = Universe::new();
    let (destination, _inbox) = universe.create_test_mailbox::<Publisher>();
    let mut template = Uploader::for_engine(
        ProbeUpload::default(),
        UploaderType::DeleteUploader,
        destination.into(),
        8,
    );
    let cloned = template.clone();
    assert!(cloned.tasks.is_empty());
    // Preserve the existing delete-task supervisor's cumulative counter sharing.
    assert!(Arc::ptr_eq(
        &template.counters.num_uploaded_splits,
        &cloned.counters.num_uploaded_splits
    ));
    template.tasks.spawn(std::future::pending());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| template.clone())).is_err());
    template.tasks.abort_all();
    assert!(
        template
            .tasks
            .join_next()
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    universe.assert_quit().await;
}

#[tokio::test]
async fn failures_and_panics_fault_direct_publication_without_a_sequencer() {
    for (stage_failure, upload_failure, panic) in [
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ] {
        let universe = Universe::new();
        let engine = ProbeUpload {
            stage_failure,
            upload_failure,
            panic,
            gate: Arc::new(Semaphore::new(1)),
            ..Default::default()
        };
        let (destination, inbox) = universe.create_test_mailbox::<Publisher>();
        let actor = Uploader::for_engine(
            engine.clone(),
            UploaderType::IndexUploader,
            destination.into(),
            8,
        );
        let (mailbox, handle) = universe.spawn_builder().spawn(actor);
        mailbox.send_message(engine.batch()).await.unwrap();
        let (status, counters) = tokio::time::timeout(Duration::from_secs(10), handle.join())
            .await
            .unwrap();
        assert!(
            !status.is_success(),
            "failed upload must fail the actor: {status:?}"
        );
        assert_eq!(
            counters.num_staged_splits.load(Ordering::SeqCst),
            u64::from(!stage_failure)
        );
        assert_eq!(counters.num_uploaded_splits.load(Ordering::SeqCst), 0);
        assert_eq!(engine.released.load(Ordering::SeqCst), 1);
        assert!(inbox.drain_for_test_typed::<SplitsUpdate>().is_empty());
        if panic {
            assert!(matches!(status, ActorExitStatus::Panicked));
            let statuses = universe.quit().await;
            assert_eq!(statuses.len(), 1);
            assert!(
                statuses
                    .values()
                    .all(|status| matches!(status, ActorExitStatus::Panicked))
            );
        } else {
            universe.assert_quit().await;
        }
    }
}

#[tokio::test]
async fn cancellation_joins_workers_and_releases_resources() {
    let universe = Universe::new();
    let engine = ProbeUpload {
        gate: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (destination, inbox) = universe.create_test_mailbox::<Publisher>();
    let actor = Uploader::for_engine(
        engine.clone(),
        UploaderType::IndexUploader,
        destination.into(),
        8,
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(actor);
    mailbox.send_message(engine.batch()).await.unwrap();
    entered(&engine).await;
    assert_eq!(engine.released.load(Ordering::SeqCst), 0);
    let (status, counters) = tokio::time::timeout(Duration::from_secs(10), handle.kill())
        .await
        .unwrap();
    assert!(!status.is_success());
    assert_eq!(
        engine.released.load(Ordering::SeqCst),
        1,
        "worker must be gone when actor joins"
    );
    assert_eq!(counters.num_uploaded_splits.load(Ordering::SeqCst), 0);
    assert!(inbox.drain_for_test_typed::<SplitsUpdate>().is_empty());
    universe.assert_quit().await;
}

#[tokio::test]
async fn successful_shutdown_drains_uploads() {
    let universe = Universe::new();
    let engine = ProbeUpload {
        gate: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (destination, inbox) = universe.create_test_mailbox::<Publisher>();
    let actor = Uploader::for_engine(
        engine.clone(),
        UploaderType::IndexUploader,
        destination.into(),
        8,
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(actor);
    mailbox.send_message(engine.batch()).await.unwrap();
    entered(&engine).await;
    mailbox
        .send_message_with_high_priority(Command::ExitWithSuccess)
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), handle.wait())
            .await
            .is_err()
    );
    assert_eq!(engine.released.load(Ordering::SeqCst), 0);
    engine.gate.add_permits(1);
    let (status, counters) = tokio::time::timeout(Duration::from_secs(10), handle.join())
        .await
        .unwrap();
    assert!(status.is_success(), "{status:?}");
    assert_eq!(counters.num_uploaded_splits.load(Ordering::SeqCst), 1);
    assert_eq!(engine.released.load(Ordering::SeqCst), 1);
    assert_eq!(inbox.drain_for_test_typed::<SplitsUpdate>().len(), 1);
    universe.assert_quit().await;
}

#[tokio::test]
async fn cancelled_publication_discards_its_ordered_slot() {
    use crate::actors::sequencer::{Sequencer, SequencerCommand};
    let universe = Universe::new();
    let engine = ProbeUpload {
        gate: Arc::new(Semaphore::new(1)),
        ..Default::default()
    };
    let (destination, inbox) = universe.create_test_mailbox::<Sequencer<Publisher>>();
    let actor = Uploader::for_engine(
        engine.clone(),
        UploaderType::IndexUploader,
        destination.into(),
        8,
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(actor);
    let batch = engine.batch();
    batch.lock.kill().await;
    mailbox.send_message(batch).await.unwrap();
    handle.process_pending_and_observe().await;
    let mut slots = inbox
        .drain_for_test_typed::<tokio::sync::oneshot::Receiver<SequencerCommand<SplitsUpdate>>>();
    assert_eq!(slots.len(), 1);
    assert!(matches!(
        slots.pop().unwrap().await.unwrap(),
        SequencerCommand::Discard
    ));
    assert_eq!(engine.entered.load(Ordering::SeqCst), 0);
    assert_eq!(engine.released.load(Ordering::SeqCst), 1);
    universe.assert_quit().await;
}

#[tokio::test]
async fn upload_failure_while_draining_cannot_report_success() {
    let universe = Universe::new();
    let engine = ProbeUpload {
        upload_failure: true,
        ..Default::default()
    };
    let (destination, inbox) = universe.create_test_mailbox::<Publisher>();
    let actor = Uploader::for_engine(
        engine.clone(),
        UploaderType::IndexUploader,
        destination.into(),
        8,
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(actor);
    mailbox.send_message(engine.batch()).await.unwrap();
    entered(&engine).await;
    mailbox
        .send_message_with_high_priority(Command::ExitWithSuccess)
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), handle.wait())
            .await
            .is_err()
    );
    engine.gate.add_permits(1);
    let (status, counters) = tokio::time::timeout(Duration::from_secs(10), handle.join())
        .await
        .unwrap();
    // The actor framework classifies finalization errors as Panicked, not Success.
    assert!(matches!(status, ActorExitStatus::Panicked), "{status:?}");
    assert_eq!(counters.num_staged_splits.load(Ordering::SeqCst), 1);
    assert_eq!(counters.num_uploaded_splits.load(Ordering::SeqCst), 0);
    assert_eq!(engine.released.load(Ordering::SeqCst), 1);
    assert!(inbox.drain_for_test_typed::<SplitsUpdate>().is_empty());
    let statuses = universe.quit().await;
    assert_eq!(statuses.len(), 1);
    assert!(
        statuses
            .values()
            .all(|status| matches!(status, ActorExitStatus::Panicked))
    );
}
