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
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task;

mod engine_environment;

// Temporarily placed a lot of logic in here for creating the TCP server, handling client requests, delegating them to the engine for processing, etc
// All of this will be stripped into separate crates/modules, but for now it will do.
#[tokio::main]
async fn main() {
    // Setup the environment of the database server - for now, we only need to create the environment
    let e = Arc::new(EngineEnvironment::new());

    // Create a dummy page just so we have something in the buffer the clients can use
    let page_id = PageId::new(1, 1);

    {
        let mut page = e.storage.new_page(page_id);
        let Ok(()) = page.initialize(page_id, PageType::Unsorted) else {
            panic!("cannot set page type")
        };
    } // Drop the write guard on the page

    // We want to limit the number of concurrent queries that are actively being executed
    // Rather than creating a fixed-sized thread pool, we will use a semaphore to allow access to the
    // code region that spawns a new worker thread.
    // 8 is right now arbitrary and obv not the final version.
    let semaphore = Arc::new(Semaphore::new(8));

    // Start listening
    let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    println!("listening on {:?}", listener.local_addr());

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        let env_clone = e.clone();
        let semaphore_clone = semaphore.clone();

        // Asynchronously handle the new connection
        tokio::spawn(async move {
            handle_client(socket, env_clone, semaphore_clone).await;
        });
    }
}

async fn handle_client(
    mut socket: TcpStream,
    env: Arc<EngineEnvironment>,
    semaphore: Arc<Semaphore>,
) {
    println!("client connected on {:?}", socket.peer_addr());

    // for now the client only sends a u32.
    let mut buf = [0u8; 4];
    if socket.read_exact(&mut buf).await.is_err() {
        eprintln!(
            "error while reading data from socket for client {:?}",
            socket.peer_addr()
        );
        return;
    }
    let _value = u32::from_le_bytes(buf);

    // Ask for a permit from the semaphore
    let permit = semaphore.acquire().await.unwrap();

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

    // Send the result back to the client. For now we only get a Vec<u8> from the worker thread, for testing purposes only. Normally we would get a result set that would be streamed as it is being processed.
    if socket.write_all(&row).await.is_err() {
        eprintln!("error while writing result")
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
