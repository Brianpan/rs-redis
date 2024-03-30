mod engine;
mod store;

use clap::{Arg, Command};
use engine::commands::array_to_resp_array;
use engine::connection::handle_connection;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use store::engine::StoreEngine;
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
    let redis_host = format!("0.0.0.0:{}", redis_port);
    let sleep_time = Duration::from_millis(3);

    let listener = TcpListener::bind(redis_host).await.unwrap();
    let db = Arc::new(StoreEngine::new());

    // collect replicaof argument
    if let Some(replica_info) = args.get_many::<String>("replicaof") {
        let values: Vec<&String> = replica_info.collect();
        let replica_host = format!("{}:{}", values[0], values[1]);
        db.set_replica(replica_host);
        // phase 1: send PING to master
        let ping_cmd = array_to_resp_array(vec!["PING".to_string()]);
        let _ = db.send_resp_to_master(ping_cmd);
        tokio::time::sleep(sleep_time).await;
        // phase 2-1: send REPLCONF listening-port
        let replconf_cmd = array_to_resp_array(vec![
            "REPLCONF".to_string(),
            "listening-port".to_string(),
            redis_port.clone(),
        ]);
        let _ = db.send_resp_to_master(replconf_cmd);
        tokio::time::sleep(sleep_time).await;

        // pase 2-2: send REPLCONF capa psync2
        let replconf_capa_cmd = array_to_resp_array(vec![
            "REPLCONF".to_string(),
            "capa".to_string(),
            "psync2".to_string(),
        ]);
        let _ = db.send_resp_to_master(replconf_capa_cmd);
    }

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
