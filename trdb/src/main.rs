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

mod engine_environment;

fn main() {
    let _ = EngineEnvironment::new();

    println!("Starting up TRDB server...");
}
