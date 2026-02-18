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

use crate::engine_environment::EngineEnvironment;
use page::insertion_plan::InsertionSlot;
use page::page_id;
use page::page_id::PageId;
use page::page_type::PageType;
use std::error::Error;
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

mod engine_environment;

// Temporarily placed a lot of logic in here for creating the TCP server, handling client requests, delegating them to the engine for processing, etc
// All of this will be stripped into separate crates/modules, but for now it will do.
#[tokio::main]
async fn main() {
    let logging_guard = init_logging("./logs");

    let e = Arc::new(EngineEnvironment::new());

    // dummy page
    let page_id = PageId::new(1, 1);
    {
        let mut page = e.storage.new_page(page_id);
        let Ok(()) = page.initialize(page_id, PageType::Unsorted) else {
            panic!("cannot set page type")
        };
    }

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
    mut socket: TcpStream,
    env: Arc<EngineEnvironment>,
    semaphore: Arc<Semaphore>,
    shutdown: CancellationToken,
) {
    tracing::info!("client connected on {:?}", socket.peer_addr());

    // for now the client only sends a u32.
    let mut buf = [0u8; 4];

    let read_res = tokio::select! {
        _ = shutdown.cancelled() => {
            tracing::info!("shutdown: stop reading new requests");
            return;
        }
        r = socket.read_exact(&mut buf) => r,
    };

    if read_res.is_err() {
        tracing::error!(
            "error while reading data from socket for client {:?}",
            socket.peer_addr()
        );
        return;
    }
    let _value = u32::from_le_bytes(buf);

    // Ask for a permit from the semaphore
    let permit = tokio::select! {
        _ = shutdown.cancelled() => {
            tracing::info!("shutdown: refuse starting new query");
            return;
        }
        p = semaphore.acquire() => p.unwrap(),
    };
    // Start processing the client request on a separate, blocking thread
    // a small note here: we request the work on a separate thread and wait for it to deliver a final result - this is fine for testing right now,
    // but it's not the end goal: normally, we would want the worker thread to deliver results as they are processed, so that we can start streaming
    // the result rows back to the client even if the worker is not yet fully done - once we have here enough rows to fill a packet, we can start shipping it via the TCP socket.
    let env_clone = env.clone();
    let row = task::spawn_blocking(move || process_query(env_clone, _value))
        .await
        .unwrap(); // await here to yield this thread back to the executor, while the query is being processed.

    // drop the semaphore permit - we are done with the worker thread, so others can use it while we stream the results back to the client via the TCP stream
    drop(permit);

    // Send the result back to the client. For now, we only get a Vec<u8> from the worker thread, for testing purposes only. Normally we would get a result set that would be streamed as it is being processed.
    if socket.write_all(&row).await.is_err() {
        tracing::error!("error while writing result")
    }
}

fn process_query(e: Arc<EngineEnvironment>, number: u32) -> Vec<u8> {
    // Read the page with the hardcoded ID
    let mut page = e.storage.write_page(PageId::new(1, 1));

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

    // Re-read the row to ensure it was inserted
    let Ok(row) = page.row(number - 1) else {
        panic!("row 0 failed");
    };

    // Return the raw row binary data
    Vec::from(row)
}

/// Sets up the logging for the server
pub fn init_logging(log_dir: &str) -> Result<WorkerGuard, Box<dyn Error + Send + Sync>> {
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
