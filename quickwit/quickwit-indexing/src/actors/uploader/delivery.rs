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

use anyhow::bail;
use quickwit_actors::{Actor, ActorContext, Mailbox};
use tokio::sync::oneshot;

use crate::actors::publisher::{Publication, PublicationEngine, Publisher};
use crate::actors::sequencer::{Sequencer, SequencerCommand};

/// Typed publication edge. Indexing reserves a sequencer slot before concurrent
/// work starts; merge/delete pipelines may deliver directly when ordering is irrelevant.
#[derive(Clone)]
pub enum PublicationMailbox<E: PublicationEngine> {
    Sequencer(Mailbox<Sequencer<Publisher<E>>>),
    Publisher(Mailbox<Publisher<E>>),
}

impl<E: PublicationEngine> std::fmt::Debug for PublicationMailbox<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sequencer(mailbox) => formatter.debug_tuple("Sequencer").field(mailbox).finish(),
            Self::Publisher(mailbox) => formatter.debug_tuple("Publisher").field(mailbox).finish(),
        }
    }
}

impl<E: PublicationEngine> From<Mailbox<Publisher<E>>> for PublicationMailbox<E> {
    fn from(mailbox: Mailbox<Publisher<E>>) -> Self {
        Self::Publisher(mailbox)
    }
}
impl<E: PublicationEngine> From<Mailbox<Sequencer<Publisher<E>>>> for PublicationMailbox<E> {
    fn from(mailbox: Mailbox<Sequencer<Publisher<E>>>) -> Self {
        Self::Sequencer(mailbox)
    }
}

impl<E: PublicationEngine> PublicationMailbox<E> {
    pub(super) async fn reserve<A: Actor>(
        &self,
        ctx: &ActorContext<A>,
    ) -> anyhow::Result<PublicationSender<E>> {
        match self {
            Self::Sequencer(mailbox) => {
                let (tx, rx) = oneshot::channel();
                ctx.send_message(mailbox, rx).await?;
                Ok(PublicationSender::Sequencer(tx))
            }
            Self::Publisher(mailbox) => Ok(PublicationSender::Publisher(mailbox.clone())),
        }
    }
}

pub(super) enum PublicationSender<E: PublicationEngine> {
    Sequencer(oneshot::Sender<SequencerCommand<Publication<E>>>),
    Publisher(Mailbox<Publisher<E>>),
}

impl<E: PublicationEngine> PublicationSender<E> {
    pub(super) fn discard(self) -> anyhow::Result<()> {
        if let Self::Sequencer(tx) = self
            && tx.send(SequencerCommand::Discard).is_err()
        {
            bail!("failed to discard publication: sequencer is closed");
        }
        Ok(())
    }

    pub(super) async fn send<A: Actor>(
        self,
        update: Publication<E>,
        ctx: &ActorContext<A>,
    ) -> anyhow::Result<()> {
        match self {
            Self::Sequencer(tx) => {
                if tx.send(SequencerCommand::Proceed(update)).is_err() {
                    bail!("failed to deliver publication: sequencer is closed");
                }
            }
            Self::Publisher(mailbox) => {
                ctx.send_message(&mailbox, update).await?;
            }
        }
        Ok(())
    }
}
