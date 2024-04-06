use tokio::sync::{mpsc, oneshot};

pub enum ReplicatorActorMessage {
    SetOp {
        cmd: String,
        respond_to: oneshot::Sender<bool>,
    },
}
pub struct ReplicatorActor {
    receiver: mpsc::Receiver<ReplicatorActorMessage>,
}

pub struct ReplicatorHandle {
    sender: mpsc::Sender<ReplicatorActorMessage>,
}

impl ReplicatorActor {
    pub fn new(receiver: mpsc::Receiver<ReplicatorActorMessage>) -> Self {
        ReplicatorActor { receiver }
    }

    pub fn handle(&mut self, msg: ReplicatorActorMessage) {
        println!("replicator received message");
        match msg {
            ReplicatorActorMessage::SetOp { cmd, respond_to } => {
                println!("replicator received command: {}", cmd);
                let _ = respond_to.send(true);
            }
        }
    }
}

async fn run_replicator(mut actor: ReplicatorActor) {
    println!("replicator started");
    while let Some(msg) = actor.receiver.recv().await {
        println!("replicator handling message");
        actor.handle(msg);
    }
}

impl ReplicatorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(128);
        let actor = ReplicatorActor::new(receiver);
        println!("new replicator handle created");
        tokio::spawn(run_replicator(actor));

        Self { sender }
    }

    pub async fn set_op(&self, cmd: String) -> bool {
        println!("set_op called");
        let (tx, rx) = oneshot::channel();
        let msg = ReplicatorActorMessage::SetOp {
            cmd,
            respond_to: tx,
        };

        let _ = self.sender.send(msg).await;

        rx.await.expect("Failed to receive response")
    }
}
