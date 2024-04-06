use super::engine::StoreEngine;
use super::master_engine::MasterEngine;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub enum ReplicatorActorMessage {
    SetOp {
        cmd: String,
        respond_to: oneshot::Sender<bool>,
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
                println!("replicator received command: {}", cmd);
                let _ = self.db.sync_command(cmd).await;
                let _ = respond_to.send(true);
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
}
