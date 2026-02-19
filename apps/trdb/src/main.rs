//! # TRDB Server
//!
//! This is the main entry point for the **TRDB** database server.
//!
//! The server is composed of multiple internal components organized under
//! the `/crates` directory of this workspace:
//!
//! - `/storage`: Core storage engine handling data persistence and retrieval.
//!
//! This binary is the main executable for the database engine.

#![allow(unused)] // Silence compiler warnings about unused code until they are referenced in main binary. TODO: remove this

use crate::config::EngineConfig;
use crate::engine_environment::EngineEnvironment;
use page::page_id::PageId;
use page::page_type::PageType;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    EnvFilter, filter::LevelFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

mod config;
mod engine_environment;

// Temporarily placed a lot of logic in here for creating the TCP server, handling client requests, delegating them to the engine for processing, etc
// All of this will be stripped into separate crates/modules, but for now it will do.
#[tokio::main]
async fn main() {
    let cfg = match EngineConfig::load_from_file("trdb.toml") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };

    let logging_guard = init_logging(&cfg.storage.logs_dir);

    let e = Arc::new(EngineEnvironment::new(cfg));

    e.setup_test_data();

    // dummy page
    let page_id = PageId::new(1, 0);
    match e.storage.read_page(page_id) {
        Ok(existing_page) => {
            tracing::info!("Found page with ID {:?}", page_id);
            let slot_count = existing_page.slot_count().unwrap();
            tracing::info!("Found {slot_count} rows");
            for i in 0..slot_count {
                let r = existing_page.row(i as u32).unwrap();
                tracing::info!("Row from slot {i}: {:?}", r)
            }
        }
        Err(err) => {
            tracing::info!("Did not find page with ID {:?}, creating it...", page_id);
            let mut new_page = e.storage.new_page(page_id).unwrap();
            new_page.initialize(page_id, PageType::Unsorted).unwrap();
            e.storage.write_page(page_id, new_page);
        }
    };

    let semaphore = Arc::new(Semaphore::new(8));
    let shutdown = CancellationToken::new();

    // Spawn a task that waits for OS shutdown signals and triggers cancellation.
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            wait_for_shutdown_signal().await;
            tracing::info!("shutdown signal received; beginning graceful shutdown");
            shutdown.cancel();
        });
    }

    let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    tracing::info!("listening on {:?}", listener.local_addr());

    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                // Stop accepting new connections.
                tracing::info!("stop accepting new connections");
                break;
            }

            res = listener.accept() => {
                let (socket, addr) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        if shutdown.is_cancelled() { break; }
                        tracing::warn!("accept failed: {e}");
                        continue;
                    }
                };

                let env_clone = e.clone();
                let semaphore_clone = semaphore.clone();
                let shutdown_clone = shutdown.clone();

                connections.spawn(async move {
                    tracing::info!("client connected: {addr}");
                    handle_client(socket, env_clone, semaphore_clone, shutdown_clone).await;
                    tracing::info!("client disconnected: {addr}");
                });
            }
        }
    }

    tracing::info!("waiting for existing connections to finish");
    while let Some(res) = connections.join_next().await {
        if let Err(join_err) = res {
            tracing::warn!("connection task ended with error: {join_err}");
        }
    }

    drop(logging_guard);
    tracing::info!("shutdown complete");
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigint = signal(SignalKind::interrupt()).expect("sigint handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("sigterm handler");

        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.expect("ctrl_c handler");
    }
}

async fn handle_client(
    socket: TcpStream,
    env: Arc<EngineEnvironment>,
    semaphore: Arc<Semaphore>,
    shutdown: CancellationToken,
) {
    // Capture peer address early for logging
    let peer = socket.peer_addr().ok();
    tracing::info!("client connected on {:?}", peer);

    // Split the socket so we can read and write concurrently from different tasks.
    let (mut reader, writer) = socket.into_split();

    // mpsc channel for workers to send completed rows to the writer task
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    // Spawn a dedicated writer task that serializes all writes to the connection
    let peer_for_writer = peer;
    let writer_handle = tokio::spawn(async move {
        let mut writer = writer;
        while let Some(msg) = rx.recv().await {
            if let Err(e) = writer.write_all(&msg).await {
                tracing::error!(
                    "error while writing result to client {:?}: {}",
                    peer_for_writer,
                    e
                );
                break;
            }
        }
        tracing::info!("writer task exiting for client {:?}", peer_for_writer);
    });

    // Serve multiple requests over the same connection until the client disconnects or shutdown is triggered
    loop {
        // for now the client only sends a u32.
        let mut buf = [0u8; 4];

        let read_res = tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("shutdown: stop reading new requests");
                return;
            }
            r = reader.read_exact(&mut buf) => r,
        };

        if read_res.is_err() {
            tracing::error!("error while reading data from socket for client {:?}", peer);
            break;
        }

        let value = u32::from_le_bytes(buf);
        tracing::info!("Received {} from {:?}", value, peer);

        // Acquire an owned permit so it can be moved into the background worker
        let permit = tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("shutdown: refuse starting new query");
                return;
            }
            p = semaphore.clone().acquire_owned() => p.unwrap(),
        };

        // Clone what the worker needs
        let tx_clone = tx.clone();
        let env_clone = env.clone();

        // proposed (reader waits for query to finish before continuing)
        let row = task::spawn_blocking(move || process_query(env_clone, value))
            .await
            .unwrap();

        if tx.send(row).is_err() {
            tracing::warn!("failed to send row to writer: receiver closed for client");
        }

        drop(permit);
    }

    // Reader is done (client disconnected or error); drop tx to signal writer to finish
    drop(tx);

    // Wait for writer task to finish before returning
    if let Err(e) = writer_handle.await {
        tracing::warn!("writer task join error: {e}");
    }

    tracing::info!("client handler exiting for {:?}", peer);
}

fn process_query(e: Arc<EngineEnvironment>, number: u32) -> Vec<u8> {
    // Read the page with the hardcoded ID
    let page_id = PageId::new(1, 0);
    let mut page = e.storage.read_page_mut(page_id).unwrap();

    // Attempt an insert of a 100bytes row
    let Ok(insert_plan) = page.plan_insert(100) else {
        panic!("insert plan failed");
    };

    // Create a row composed of 100 bytes with the provided number and insert it
    let byte_value = number as u8;
    match page.insert_heap(insert_plan, vec![byte_value; 100]) {
        Ok(_) => {}
        Err(e) => panic!("insert failed: {}", e),
    }
    e.storage.write_page(page_id, page);

    let page = e.storage.read_page(page_id).unwrap();

    // Re-read the row to ensure it was inserted
    let Ok(row) = page.row(number - 1) else {
        panic!("row 0 failed");
    };

    // Return the raw row binary data
    Vec::from(row)
}

/// Sets up the logging for the server
pub fn init_logging(log_dir: &PathBuf) -> Result<WorkerGuard, Box<dyn Error + Send + Sync>> {
    let file_appender = tracing_appender::rolling::daily(log_dir, "trdb.log");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let console_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_target(false)
        .with_level(true)
        .compact();

    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .json()
        .with_current_span(true)
        .with_span_list(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer)
        .init();

    Ok(guard)
}
