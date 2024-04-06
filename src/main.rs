mod engine;
mod store;

use clap::{Arg, Command};
use engine::connection::handle_connection;
use std::sync::Arc;
use store::engine::StoreEngine;

use tokio::sync::Mutex;
use tokio::{net::TcpListener, spawn};

const PROGRAM_NAME: &str = "rs-redis";
const VERSION: &str = "0.1.0";
const DEFAULT_PORT: &str = "6379";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // configure the command line arguments
    let args = Command::new(PROGRAM_NAME)
        .version(VERSION)
        .about("redis in rust")
        .arg(
            Arg::new("port")
                .help("Redis port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .required(false),
        )
        .arg(
            Arg::new("replicaof")
                .help("replacate from another host:port")
                .long("replicaof")
                .value_names([&"HOST", &"PORT"])
                .number_of_values(2)
                .required(false),
        )
        .get_matches();

    let binding = DEFAULT_PORT.to_string();
    let redis_port = args.get_one::<String>("port").unwrap_or(&binding);
    let redis_host: String = format!("0.0.0.0:{}", redis_port);

    let listener = TcpListener::bind(redis_host).await.unwrap();
    let db = Arc::new(StoreEngine::new());

    db.set_node_info(redis_port.clone());

    // collect replicaof argument
    if let Some(replica_info) = args.get_many::<String>("replicaof") {
        let values: Vec<&String> = replica_info.collect();
        let replica_host = format!("{}:{}", values[0], values[1]);
        db.set_replica(replica_host);

        let db = db.clone();
        spawn(async move {
            let _ = db.handshake_to_master().await;
        });
    }

    // reaper thread
    let reaper_db = db.clone();
    spawn(async move {
        reaper_db.expired_reaper().await;
    });

    println!("start accept");

    while let Ok((socket, addr)) = listener.accept().await {
        let cdb = db.clone();
        // let std_stream = socket.into_std()?;
        let stream = Arc::new(Mutex::new(socket));
        println!("New accept from: {}", addr.clone());
        tokio::spawn(async move { handle_connection(&cdb, stream, addr).await });
    }

    Ok(())
}
