mod engine;
mod store;

use engine::connection::handle_connection;
use std::sync::Arc;
use store::engine::StoreEngine;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(StoreEngine::new());
    loop {
        let cdb = db.clone();
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(&cdb, socket).await;
        });
    }
}
