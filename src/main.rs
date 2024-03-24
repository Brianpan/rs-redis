mod engine;
mod store;

use engine::connection::handle_connection;
use std::sync::Arc;
use store::engine::StoreEngine;
use tokio::{net::TcpListener, spawn};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(StoreEngine::new());

    // reaper thread
    let reaper_db = db.clone();
    spawn(async move {
        reaper_db.expired_reaper().await;
    });

    loop {
        let cdb = db.clone();
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(&cdb, socket).await;
        });
    }
}
