use super::engine::StoreEngine;
use super::master_engine::MasterEngine;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub enum ReplicatorActorMessage {
    SetOp {
        cmd: String,
        respond_to: oneshot::Sender<bool>,
    },
    GetAck {
        respond_to: oneshot::Sender<bool>,
    },
    Wait {
        wait_count: u64,
        wait_time: u64,
        respond_to: oneshot::Sender<u32>,
    },
}
pub struct ReplicatorActor {
    db: Arc<StoreEngine>,
    receiver: mpsc::Receiver<ReplicatorActorMessage>,
}

pub struct ReplicatorHandle {
    sender: mpsc::Sender<ReplicatorActorMessage>,
}

impl ReplicatorActor {
    pub fn new(db: Arc<StoreEngine>, receiver: mpsc::Receiver<ReplicatorActorMessage>) -> Self {
        ReplicatorActor { db, receiver }
    }

    pub async fn handle(&mut self, msg: ReplicatorActorMessage) {
        match msg {
            ReplicatorActorMessage::SetOp { cmd, respond_to } => {
                let _ = self.db.sync_command(cmd).await;
                let _ = respond_to.send(true);
            }
            ReplicatorActorMessage::GetAck { respond_to } => {
                let _ = self.db.send_ack_to_slave().await;
                let _ = respond_to.send(true);
            }
            ReplicatorActorMessage::Wait {
                wait_time,
                wait_count,
                respond_to,
            } => {
                let count = self.db.wait_replica(wait_time, wait_count).await;
                let _ = respond_to.send(count);
            }
        }
    }
}

async fn run_replicator(mut actor: ReplicatorActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle(msg).await;
    }
}

impl ReplicatorHandle {
    pub fn new(db: Arc<StoreEngine>) -> Self {
        let (sender, receiver) = mpsc::channel(128);
        let actor = ReplicatorActor::new(db, receiver);
        tokio::spawn(run_replicator(actor));

        Self { sender }
    }

    pub async fn set_op(&self, cmd: String) -> bool {
        let (tx, rx) = oneshot::channel();
        let msg = ReplicatorActorMessage::SetOp {
            cmd,
            respond_to: tx,
        };

        let _ = self.sender.send(msg).await;

        rx.await.expect("Failed to receive response")
    }

    pub async fn wait_op(&self, wait_count: u64, wait_time: u64) -> u32 {
        let (tx, rx) = oneshot::channel();
        let msg = ReplicatorActorMessage::Wait {
            wait_count,
            wait_time,
            respond_to: tx,
        };

        let _ = self.sender.send(msg).await;

        rx.await.expect("Failed to receive response")
    }

    pub async fn getack_op(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        let msg = ReplicatorActorMessage::GetAck { respond_to: tx };

        let _ = self.sender.send(msg).await;

        rx.await.expect("Failed to receive response")
    }
}
